package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.broker.BrokerBG.Role.*
import de.hasenburg.broker.simulation.main.stack.BTarget
import de.hasenburg.broker.simulation.main.stack.CTarget
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.channels.SendChannel

/**
 * A broker that follows the Broadcast Group strategy.
 * Each broker has one of three [Role]s: member, leader, and Cloud; it is set via [assignRole].
 *
 * The role is determined and set by the [BrokerBGManager]. While member and leaders are based on the input data,
 * the Cloud broker is always at a fixed location and added to the set of existing brokers (so number of brokers is n+1).
 */
class BrokerBG(brokerId: BrokerId, location: Location) : Broker(brokerId, location) {

    private lateinit var role: Role

    /**
     * Must be called before anything uses this broker.
     * Similarly to [wireBrokerDirectory], this should be done before using the broker.
     */
    fun assignRole(role: Role) {
        check(!this::role.isInitialized) { "Role should only be set once" }
        this.role = role
    }

    enum class Role {
        Member, Leader, Cloud
    }

    /*****************************************************************/
    //region CMessage

    override fun processLocationUpdate(message: CLocationUpdate,
                                       resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        updateLocation(message.origin.id, message.newLocation)
        brokerDirectory.getBTargetsForOtherBrokers(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processSubscriptionUpdate(message: CSubscriptionUpdate,
                                           resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        when (role) {
            Member -> {
                updateSubscription(message.origin.id, message.subscription)
                val leader = brokerDirectory.bgManager().getLeader(brokerId)
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, leader)
                resultChannel.add(message.forward(BTarget(leader, now + latency)))
            }
            Leader -> {
                updateSubscription(message.origin.id, message.subscription)
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                resultChannel.add(message.forward(BTarget(cloud, now + latency)))
            }
            Cloud -> unexpectedMessageLog(message)
        }

        endLog(message)
    }

    override fun processSubscriptionRemoval(message: CSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        when (role) {
            Member -> {
                removeSubscription(message.origin.id, message.topic)
                val leader = brokerDirectory.bgManager().getLeader(brokerId)
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, leader)
                resultChannel.add(message.forward(BTarget(leader, now + latency)))
            }
            Leader -> {
                removeSubscription(message.origin.id, message.topic)
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                resultChannel.add(message.forward(BTarget(cloud, now + latency)))
            }
            Cloud -> unexpectedMessageLog(message)
        }

        endLog(message)
    }

    override fun processEventMatching(message: CEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)
        val bTarget: BTarget

        when (role) {
            Member -> {
                val leader = brokerDirectory.bgManager().getLeader(brokerId)
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, leader)
                bTarget = BTarget(leader, now + latency)
            }
            Leader -> {
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                bTarget = BTarget(cloud, now + latency)
            }
            Cloud -> {
                unexpectedMessageLog(message)
                return
            }
        }

        val subsPerBroker = matchEvent(message.origin.id, message.event)
        val subscriptions = validateSubscribersAreLocal(brokerId, subsPerBroker)

        // deliver to subscribers
        subscriptions
            .map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            .map { message.createCEventDelivery(it) }
            .forEach { resultChannel.add(it) }

        // forward EventMatching to all (other) members in group
        brokerDirectory.bgManager().getOtherGroupMembers(brokerId)
            .map { memberId -> memberId to brokerDirectory.getLatencyBetweenBrokers(brokerId, memberId) }
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
            .map { message.forward(it) } // create BEventMatching
            .forEach { resultChannel.add(it) }

        // forward EventMatching to leader/cloud (depends on role)
        resultChannel.add(message.forward(bTarget))

        endLog(message)
    }

    //endregion
    /*****************************************************************/

    /*****************************************************************/
    //region BMessage

    override fun processLocationUpdate(message: BLocationUpdate,
                                       resultChannel: SendChannel<Message>) {
        startLog(message)
        updateLocation(message.origin.id, message.newLocation)
        endLog(message)
    }

    override fun processSubscriptionUpdate(message: BSubscriptionUpdate,
                                           resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        when (role) {
            Member -> unexpectedMessageLog(message)
            Leader -> {
                check(brokerDirectory.bgManager().messageIsFromOwnMember(brokerId, message))
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                resultChannel.add(message.forward(BTarget(cloud, now + latency)))
            }
            Cloud -> {
                check(brokerDirectory.bgManager().messageIsFromLeader(message))
                updateSubscription(message.origin.id, message.subscription)
            }
        }

        endLog(message)
    }

    override fun processSubscriptionRemoval(message: BSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        when (role) {
            Member -> unexpectedMessageLog(message)
            Leader -> {
                check(brokerDirectory.bgManager().messageIsFromOwnMember(brokerId, message))
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                resultChannel.add(message.forward(BTarget(cloud, now + latency)))
            }
            Cloud -> {
                check(brokerDirectory.bgManager().messageIsFromLeader(message))
                removeSubscription(message.origin.id, message.topic)
            }
        }

        endLog(message)
    }

    override fun processEventMatching(message: BEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        when (role) {
            Member -> {
                check(brokerDirectory.bgManager().messageIsFromGroup(brokerId, message)) {
                    "$message is not from another member or the leader of this group."
                }
                val subsPerBroker = matchEvent(message.origin.id, message.event)
                val subscriptions = validateSubscribersAreLocal(brokerId, subsPerBroker)

                subscriptions
                    .map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
                    .map { message.createCEventDelivery(it) }
                    .forEach { resultChannel.add(it) }
            }
            Leader -> {
                // must be from cloud or from one of its members, checked below
                val subsPerBroker = matchEvent(message.origin.id, message.event)
                val subscriptions = validateSubscribersAreLocal(brokerId, subsPerBroker)

                subscriptions
                    .map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
                    .map { message.createCEventDelivery(it) }
                    .forEach { resultChannel.add(it) }

                // if from cloud -> broadcast into group
                if (brokerDirectory.bgManager().messageIsFromCloud(message)) {
                    brokerDirectory.bgManager().getOtherGroupMembers(brokerId)
                        .map { memberId -> memberId to brokerDirectory.getLatencyBetweenBrokers(brokerId, memberId) }
                        .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
                        .map { message.forward(it) } // create BEventMatching
                        .forEach { resultChannel.add(it) }
                    return
                }

                // is from member -> send to cloud
                check(brokerDirectory.bgManager().messageIsFromOwnMember(brokerId, message))
                val cloud = brokerDirectory.bgManager().cloudBroker.brokerId
                val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, cloud)
                resultChannel.add(message.forward(BTarget(cloud, now + latency)))
            }
            Cloud -> {
                check(brokerDirectory.bgManager().messageIsFromLeader(message))
                val subsPerBroker = matchEvent(message.origin.id, message.event)

                // get all leaders that have their own matching subscription
                val leadersWithSubscribers = subsPerBroker.keys
                    .filter { brokerDirectory.bgManager().isLeader(it) } // only leaders

                // get leaders of members with a matching subscription
                val leadersWithMembers = subsPerBroker.keys
                    .filter { brokerDirectory.bgManager().isMember(it) } // only members
                    .map { brokerDirectory.bgManager().getLeader(it) }

                // combine leaders, filter senders, and send messages
                listOf(leadersWithSubscribers, leadersWithMembers).flatten()
                    .distinct() // remove duplicates
                    .filter { it != message.brokers[0].id } // remove sender of message
                    .map { leaderId -> leaderId to brokerDirectory.getLatencyBetweenBrokers(brokerId, leaderId) }
                    .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
                    .map { message.forward(it) } // create BEventMatching
                    .forEach { resultChannel.add(it) }
            }
        }

        endLog(message)
    }

    override fun processEventDelivery(message: BEventDelivery,
                                      resultChannel: SendChannel<Message>) {
        unexpectedMessageLog(message)
    }

    //endregion
    /*****************************************************************/

}