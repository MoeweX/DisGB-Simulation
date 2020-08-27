package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Event
import de.hasenburg.broker.simulation.main.stack.CTarget
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.channels.SendChannel

class BrokerDisGBEvents(
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
        startLog(message)
        updateSubscription(message.origin.id, message.subscription)
        endLog(message)
    }

    override fun processSubscriptionRemoval(message: CSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        startLog(message)
        removeSubscription(message.origin.id, message.topic)
        endLog(message)
    }

    override fun processEventMatching(message: CEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subsPerBroker = matchEvent(message.origin.id, message.event)
        val subscriptions = validateSubscribersAreLocal(brokerId, subsPerBroker)

        // deliver to subscribers
        subscriptions
            .map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            .map { message.createCEventDelivery(it) }
            .forEach { resultChannel.add(it) }

        // forward EventMatching
        brokerDirectory.getBTargetsForOtherAffectedBrokers(brokerId, now, message.event.geofence)
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
        unexpectedMessageLog(message)
    }

    override fun processSubscriptionRemoval(message: BSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>) {
        unexpectedMessageLog(message)
    }

    override fun processEventMatching(message: BEventMatching,
                                      resultChannel: SendChannel<Message>) {
        val now = startLog(message)

        val subsPerBroker = matchEvent(message.origin.id, message.event)
        val subscriptions = validateSubscribersAreLocal(brokerId, subsPerBroker)

        subscriptions
            .map { CTarget(it, now + clientLatency) } // prepare CTargets for local subscribers
            .map { message.createCEventDelivery(it) }
            .forEach { resultChannel.add(it) }

        endLog(message)
    }

    override fun processEventDelivery(message: BEventDelivery,
                                      resultChannel: SendChannel<Message>) {
        unexpectedMessageLog(message)
    }

}