package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.*
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.channels.SendChannel
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

enum class BrokerType {
    BrokerDisGBEvents,
    BrokerDisGBSubscriptions,
    BrokerFloodingEvents,
    BrokerFloodingSubscriptions,
    BrokerDHT,
    BrokerGQPS,
    BrokerBG
}

abstract class Broker(val brokerId: BrokerId, val location: Location) {

    companion object Constants {
        /**
         * Latency for messages between the broker and clients.
         */
        const val clientLatency = 5

        init {
            check(clientLatency > 0) { "Client latency must be larger than 0" }
        }
    }

    protected val logger: Logger = LogManager.getLogger("Broker $brokerId")

    protected val clientLocations = mutableMapOf<ClientId, Location>()
    protected val subscriptions = mutableMapOf<Topic, SubscriptionsPerTopic>()

    protected lateinit var brokerDirectory: BrokerDirectory

    /**
     * Must be called before anything accesses the broker directory.
     * This should be done in the [BrokerDirectory] directly, e.g., in an init block.
     */
    fun wireBrokerDirectory(brokerDirectory: BrokerDirectory) {
        this.brokerDirectory = brokerDirectory
    }

    /*****************************************************************
     * Logging
     ****************************************************************/

    /**
     * Logs that processing of the [message] has been started.
     * @return the current [Tick]
     */
    fun startLog(message: Message): Tick {
        val now = message.now
        val us = message.us

        require(brokerId == us) { "We ($brokerId) should not process message of $us" }
        logger.trace("[$now]: Processing ${message::class.java.simpleName}: $message")
        return now
    }

    /**
     * Logs that the given [message] was processed.
     */
    fun endLog(message: Message) {
        logger.trace("Processed ${message::class.java.simpleName}")
    }

    /**
     * Logs that the given [message] is unexepcted and should not have arrived.
     */
    fun unexpectedMessageLog(message: Message) {
        logger.error("There should never be a ${message::class.java.simpleName}, but received $message")
    }

    /*****************************************************************
     * Extension functions
     ****************************************************************/

    fun SendChannel<Message>.add(message: Message) {
        check(this.offer(message)) { "Result channel did not accept ${message::class.java.simpleName} message!" }
    }

    protected fun ClientId.getLocalBroker(): BrokerId {
        val clientLocation = clientLocations[this] ?: error("Client is missing a location")
        return brokerDirectory.getLocalBroker(clientLocation)
    }

    /*****************************************************************
     * CMessage
     ****************************************************************/

    abstract fun processLocationUpdate(message: CLocationUpdate,
                                       resultChannel: SendChannel<Message>)

    abstract fun processSubscriptionUpdate(message: CSubscriptionUpdate,
                                           resultChannel: SendChannel<Message>)

    abstract fun processSubscriptionRemoval(message: CSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>)

    abstract fun processEventMatching(message: CEventMatching,
                                      resultChannel: SendChannel<Message>)

    /**
     * This message is called during the same tick the client receives the message, as in theory the client should
     * be the one processing the delivered event rather than a broker.
     * See also [CEventDelivery].
     */
    fun processEventDelivery(message: CEventDelivery) {
        check(message.subscriber.id.getLocalBroker() == brokerId) {
            "Only the local broker should deliver a message to a subscriber, but got: $message"
        }
        logger.debug("Delivered event from ${message.origin} to ${message.subscriber}.")
    }

    /*****************************************************************
     * BMessage
     ****************************************************************/

    abstract fun processLocationUpdate(message: BLocationUpdate,
                                       resultChannel: SendChannel<Message>)

    abstract fun processSubscriptionUpdate(message: BSubscriptionUpdate,
                                           resultChannel: SendChannel<Message>)

    abstract fun processSubscriptionRemoval(message: BSubscriptionRemoval,
                                            resultChannel: SendChannel<Message>)

    abstract fun processEventMatching(message: BEventMatching,
                                      resultChannel: SendChannel<Message>)

    abstract fun processEventDelivery(message: BEventDelivery,
                                      resultChannel: SendChannel<Message>)

    /*****************************************************************
     * Common message processing operations
     ****************************************************************/

    /**
     * Updates the location of [clientId] to [newLocation] at this broker.
     */
    protected fun updateLocation(clientId: ClientId, newLocation: Location) {
        val old = clientLocations.put(clientId, newLocation)
        if (old == null) {
            logger.debug("Added location $newLocation for client $clientId")
        } else {
            logger.debug("Updated location to $newLocation for client $clientId; was $old")
        }
    }

    /**
     * Updates the subscription of [clientId] to [subscription] at this broker.
     * If there already exists a subscription with the same topic, returns the corresponding former geofence.
     */
    protected fun updateSubscription(clientId: ClientId, subscription: Subscription): Geofence? {
        val t = subscription.topic
        // logging happens in the updateSubscription method
        return subscriptions.getOrPut(t) { SubscriptionsPerTopic(t) }.updateSubscription(clientId, subscription)
    }

    /**
     * Removes the subscription of [clientId] to the given [topic] at this broker.
     * If a subscription existed, returns the corresponding former geofence.
     */
    protected fun removeSubscription(clientId: ClientId, topic: Topic): Geofence? {
        // logging happens in the removeSubscription method
        return subscriptions.getOrPut(topic) { SubscriptionsPerTopic(topic) }.removeSubscription(clientId, topic)
    }

    /**
     * Matches the [event] published by [clientId] at this broker.
     * Returns the ids of all clients with matching subscriptions that should receive the event grouped by their local broker.
     */
    protected fun matchEvent(clientId: ClientId, event: Event): Map<BrokerId, Set<ClientId>> {
        val publisherLocation = clientLocations[clientId] ?: run {
            logger.error("Dropping event of publisher $clientId as it does not have supplied a location")
            return emptyMap()
        }

        // subscription topic matches event topic
        val contentChecked = subscriptions[event.topic]?.getAll() ?: emptyMap()
        logger.trace("${contentChecked.size} subscribers passed the ContentCheck for topic ${event.topic}")

        // subscription geofence contains publisher location
        val subGeoChecked = contentChecked.filter { it.value.geofence.contains(publisherLocation) }
        logger.trace("${subGeoChecked.size} subscribers passed also the subscriber GeoCheck")

        // event geofence contains subscriber location
        val matching = subGeoChecked
            .filter { clientLocations.containsKey(it.key) } // only subscribers that supplied a location
            .filter { event.geofence.contains(clientLocations[it.key]!!) } // !! is checked above
        logger.trace("${matching.size} subscribers passed also the publisher GeoCheck")

        if (matching.isEmpty() && clientId.getLocalBroker() == brokerId) {
            logger.warn("Event from $clientId has no subscribers: $event")
        }

        return matching.keys
            .map { it.getLocalBroker() to it } // brokerId -> clientId
            .groupBy { it.first } // brokerId -> List<brokerId -> clientId>
            .mapValues { it.value.map { pair -> pair.second }.toSet() } // brokerId -> List<clientIdY
    }

    fun validateSubscribersAreLocal(brokerId: BrokerId, subsPerBroker: Map<BrokerId, Set<ClientId>>): Set<ClientId> {
        if (subsPerBroker.isEmpty()) {
            return emptySet() // no subscribers
        }
        check(subsPerBroker.size == 1) { "There is more than one broker with subscribers: $subsPerBroker" }
        return subsPerBroker[brokerId] ?: error("Broker with subscribers is not $brokerId: $subsPerBroker")
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun toString(): String {
        return "Broker(brokerId=$brokerId)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Broker) return false

        if (brokerId != other.brokerId) return false
        if (location != other.location) return false
        if (clientLocations != other.clientLocations) return false
        if (subscriptions != other.subscriptions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = brokerId.hashCode()
        result = 31 * result + location.hashCode()
        result = 31 * result + clientLocations.hashCode()
        result = 31 * result + subscriptions.hashCode()
        return result
    }

}
