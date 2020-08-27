package de.hasenburg.broker.simulation.main.misc

import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import org.apache.logging.log4j.LogManager

private val logger = LogManager.getLogger()

/**
 * A [Broker] might manage multiple [Subscription] for a given [Topic] because each client can have 0 or 1
 * [Subscription] for each topic.
 *
 * Thus, each subscription is uniquely identified by a [Topic] and a [ClientId].
 */
class SubscriptionsPerTopic(private val managedTopic: Topic) {

    private val subscriptions = mutableMapOf<ClientId, Subscription>()

    /**
     * Updates the subscription.
     * If the defined [ClientId] and [Topic] had a subscription before, returns the [Geofence] of the original subscription.
     */
    fun updateSubscription(clientId: ClientId, subscription: Subscription): Geofence? {
        require(managedTopic == subscription.topic)
        val old = subscriptions.put(clientId, subscription)
        return if (old == null) {
            logger.debug("Created subscription $subscription")
            null
        } else {
            logger.debug("Updated subscription $old to $subscription")
            old.geofence
        }
    }

    fun removeSubscription(clientId: ClientId, topic: Topic): Geofence? {
        require(managedTopic == topic)
        val old = subscriptions.remove(clientId)
        return if (old == null) {
            logger.warn("Could not remove subscription for client $clientId and topic $topic, as none existed")
            null
        } else {
            logger.debug("Removed subscription $old")
            old.geofence
        }
    }

    fun getAll(): Map<ClientId, Subscription> {
        return subscriptions
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SubscriptionsPerTopic

        if (managedTopic != other.managedTopic) return false
        if (subscriptions != other.subscriptions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = managedTopic.hashCode()
        result = 31 * result + subscriptions.hashCode()
        return result
    }

}
