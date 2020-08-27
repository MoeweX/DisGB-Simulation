package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.stack.BTarget
import de.hasenburg.broker.simulation.main.stack.CTarget
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.channels.SendChannel
import kotlinx.serialization.json.internal.checkKind

class BrokerFloodingSubscriptions(
        brokerId: BrokerId,
        location: Location) : Broker(brokerId, location) {

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
        brokerDirectory.getBTargetsForOtherBrokers(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processSubscriptionRemoval(message: CSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        removeSubscription(message.origin.id, message.topic)
        brokerDirectory.getBTargetsForOtherBrokers(brokerId, now)
            .map { message.forward(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processEventMatching(message: CEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subsPerBroker = matchEvent(message.origin.id, message.event)

        // deliver to local subscribers
        subsPerBroker[brokerId]
            ?.map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            ?.map { message.createCEventDelivery(it) }
            ?.forEach { resultChannel.add(it) } ?: logger.debug("No local subscribers")

        // deliver to remote brokers so that they can deliver to their local subscribers
        for ((remoteBrokerId, subscribers) in subsPerBroker.filter { it.key != brokerId }) {
            val latency = brokerDirectory.getLatencyBetweenBrokers(brokerId, remoteBrokerId)
            val target = BTarget(remoteBrokerId, now + latency)
            resultChannel.add(message.createBEventDelivery(subscribers, target))
        }

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

    override fun processEventMatching(message: BEventMatching,
                                      resultChannel: SendChannel<Message>) {
        unexpectedMessageLog(message)
    }

    override fun processEventDelivery(message: BEventDelivery,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subscribers = message.subscribers
        subscribers.forEach {
            check(it.getLocalBroker() == brokerId) { "Not the local broker of $it" }
        }

        message.createCEventDeliveries(now + clientLatency)
            .forEach { resultChannel.add(it) }

        endLog(message)
    }
}