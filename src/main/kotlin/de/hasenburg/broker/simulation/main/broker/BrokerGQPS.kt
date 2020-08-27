package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.stack.BTarget
import de.hasenburg.broker.simulation.main.stack.CTarget
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.*
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.channels.SendChannel

/**
 * Based on the Grid Quorum-based pub/sub system (GQPS) from Sun et al. (2013).
 *
 * Notable differences/decisions:
 * - As clients communicate only with their local broker, matched events for remote subscribers are not directly sent to
 *   these subscribers. Instead, they are forwarded to their local broker (BEventDelivery).
 * - In the original approach, subscriptions at remote brokers are updated at the defined maintenance interval frequency.
 *   For the simulation, client subscriptions are immediately updated so that clients are always up-to-date data.
 */
class BrokerGQPS(brokerId: BrokerId, location: Location) : Broker(brokerId, location) {

    /*****************************************************************
     * CMessage
     ****************************************************************/

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

        updateSubscription(message.origin.id, message.subscription)
        brokerDirectory.getBTargetsForOtherBrokersInRowAndColumn(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processSubscriptionRemoval(message: CSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        removeSubscription(message.origin.id, message.topic)
        brokerDirectory.getBTargetsForOtherBrokersInRowAndColumn(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processEventMatching(message: CEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subsPerBroker = matchEvent(message.origin.id, message.event)

        // deliver to local subscribers (avoid duplicates for remote subscribers caused by BEventMatching forwarding)
        subsPerBroker[brokerId]
            ?.map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            ?.map { message.createCEventDelivery(it) }
            ?.forEach { resultChannel.add(it) } ?: logger.debug("No local subscribers")

        // forward to others in row and column
        brokerDirectory.getBTargetsForOtherBrokersInRowAndColumn(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    /*****************************************************************
     * BMessage
     ****************************************************************/

    override fun processLocationUpdate(message: BLocationUpdate,
                                       resultChannel: SendChannel<Message>) {
        startLog(message)
        updateLocation(message.origin.id, message.newLocation)
        endLog(message)
    }

    override fun processSubscriptionUpdate(message: BSubscriptionUpdate,
                                           resultChannel: SendChannel<Message>) {
        startLog(message)
        updateSubscription(message.origin.id, message.subscription)
        endLog(message)
    }

    override fun processSubscriptionRemoval(message: BSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        startLog(message)
        removeSubscription(message.origin.id, message.topic)
        endLog(message)
    }

    @Suppress("CascadeIf")
    override fun processEventMatching(message: BEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subsPerBroker = matchEvent(message.origin.id, message.event)

        // deliver to local subscribers
        subsPerBroker[brokerId]
            ?.map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            ?.map { message.createCEventDelivery(it) }
            ?.forEach { resultChannel.add(it) } ?: logger.trace("No local subscribers for BEventMatching")

        // only keep brokers in same row OR column (depending on sender of BEventMatching)
        val targets = if (brokerDirectory.gridGQPS.areInSameRow(brokerId, message.brokers[0].id)) {
            logger.trace("Received BEventMatching from another broker in same row")
            brokerDirectory.gridGQPS.getOtherBrokerIdsInSameColumn(brokerId)
        } else if (brokerDirectory.gridGQPS.areInSameColumn(brokerId, message.brokers[0].id)) {
            logger.trace("Received BEventMatching from another broker in same column")
            brokerDirectory.gridGQPS.getOtherBrokerIdsInSameRow(brokerId)
        } else {
            error("Should not receive a BEventMatching from a broker that is not in same row or same column")
        }

        // deliver to remote brokers if part of targets
        for ((remoteBrokerId, subscribers) in subsPerBroker.filter { targets.contains(it.key) }) {
            val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, remoteBrokerId)
            val target = BTarget(remoteBrokerId, now + latency)
            resultChannel.add(message.createBEventDelivery(subscribers, target))
        }

        endLog(message)
    }

    /**
     * Events that have been delivered due to a [BEventDelivery] message. Necessary, as each message will be received
     * twice.
     * Upon receiving an event the second time, the event will be removed from this set, again.
     */
    internal val deliveredEvents = mutableSetOf<CTarget>()

    override fun processEventDelivery(message: BEventDelivery,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        if (deliveredEvents.contains(message.origin)) {
            deliveredEvents.remove(message.origin)
            logger.trace("Received the duplicate of event, dropping it.")
            return
        }

        deliveredEvents.add(message.origin)
        val subscribers = message.subscribers
        subscribers.forEach {
            check(it.getLocalBroker() == brokerId) { "Not the local broker of $it" }
        }

        message.createCEventDeliveries(now + clientLatency)
            .forEach { resultChannel.add(it) }

        endLog(message)
    }
}
