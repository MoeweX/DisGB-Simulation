package de.hasenburg.broker.simulation.main.client

import de.hasenburg.broker.simulation.main.broker.BrokerDirectory
import de.hasenburg.broker.simulation.main.misc.Event
import de.hasenburg.broker.simulation.main.misc.Subscription
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.misc.Topic
import de.hasenburg.broker.simulation.main.stack.BTarget
import de.hasenburg.broker.simulation.main.stack.CTarget
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import kotlin.random.Random

private val logger = LogManager.getLogger()

data class ClientMessageHolder(val clientId: ClientId, val startLocation: Location = Location.random()) {

    companion object Constants {
        private const val minTravelSpeed = 50.0 / 60 / 60 / 1000 // 50 km/h to km/Tick
        private const val maxTravelSpeed = 100.0 / 60 / 60 / 1000 // 100 km/h to km/Tick

        // here expressed as Tick, as Ticks are always in ms
        private val maxTravelTime = Tick(60000) // 60 seconds
        private val maxWarmupTime = Tick(50000) // 50 seconds
        private val opDelay = Tick(1000) // is larger than highest latency between any two brokers
        private val latencyToBroker = Tick(5)
        private val topicSubsetSize = 5
    }

    /**
     * Returns message typical for a wandering client that stays inside the broker area of its initial broker.
     * The message travels for a maximum of [maxTravelTime] before sending a ping, subscribe, or publish message (rotates).
     */
    fun getMessagesForWanderingClient(possibleTopics: List<Topic>, maxTick: Tick, eventGeofenceSize: Double,
                                      subscriptionGeofenceSize: Double, brokerDirectory: BrokerDirectory,
                                      random: Random = Random.Default): List<Message> {
        val messages = mutableListOf<Message>()
        val clientDirection = random.nextDouble(0.0, 360.0)

        var currentTick = Tick(0).pickBefore(maxWarmupTime, random)
        var nextLocation = startLocation
        var localBrokerId = brokerDirectory.getLocalBroker(nextLocation)

        // clients use a subset of topics
        val topics = possibleTopics.shuffled(random).take(5)

        // initial ping
        messages.add(CLocationUpdate(nextLocation,
                CTarget(clientId, currentTick),
                BTarget(localBrokerId, currentTick + latencyToBroker)))
        currentTick += maxWarmupTime

        var counter = 0

        while (currentTick < maxTick) {

            if (counter % 5 == 0) {

                // ping
                logger.trace("Generating ping")
                messages.add(CLocationUpdate(nextLocation,
                        CTarget(clientId, currentTick),
                        BTarget(localBrokerId, currentTick + latencyToBroker)))
                currentTick += opDelay

                // subscriptions
                logger.trace("Generating subscriptions")
                topics.forEach {
                    val geofence = Geofence.circle(nextLocation, subscriptionGeofenceSize)
                    messages.add(CSubscriptionUpdate(Subscription(it, geofence),
                            CTarget(clientId, currentTick),
                            BTarget(localBrokerId, currentTick + latencyToBroker)))
                    currentTick++
                }
                currentTick += opDelay

            }

            // events
            logger.trace("Generating events")
            val topicIndex = random.nextInt(0, topicSubsetSize)
            topics[topicIndex].let {
                val geofence = Geofence.circle(nextLocation, eventGeofenceSize)
                messages.add(CEventMatching(Event(it, geofence),
                        CTarget(clientId, currentTick),
                        BTarget(localBrokerId, currentTick + latencyToBroker)))
                currentTick++
            }
            currentTick += opDelay

            // calculate next values
            val travelTime = Tick(0).pickBefore(currentTick + maxTravelTime, random)
            currentTick += travelTime
            nextLocation = nextLocation.calculateNextLocation(travelTime,
                    minTravelSpeed,
                    maxTravelSpeed,
                    clientDirection,
                    brokerDirectory.getBrokerArea(localBrokerId),
                    random)
            localBrokerId = brokerDirectory.getLocalBroker(nextLocation)
            counter++
        }

        logger.debug("Client $clientId finished preparing messages {}", messages.toString())
        return messages
    }

    private fun Location.calculateNextLocation(travelTime: Tick,
                                               minTravelSpeed: Double, maxTravelSpeed: Double,
                                               clientDirection: Double,
                                               brokerArea: List<Geofence>, random: Random): Location {

        val travelSpeed = random.nextDouble(minTravelSpeed, maxTravelSpeed) // km / Tick
        val distance = travelSpeed * travelTime.ms // km
        logger.trace("Travelling with $travelSpeed km/Tick for $travelTime ticks which leads to ${distance * 1000}m.")

        var nextLocation: Location
        var relaxFactor = 1.0

        while (true) {
            // choose a direction (roughly in the direction of the client
            val direction = random.nextDouble(clientDirection - 10.0 - relaxFactor, clientDirection + 10.0 + relaxFactor)

            nextLocation = this.locationInDistance(distance, direction)

            // in case we are at the edge of a geofence, we need to relax it a little bit otherwise this will be an
            // infinite loop
            relaxFactor += 1.0

            if (relaxFactor > 30) {
                // let's go back by 180 degree
                nextLocation = this.locationInDistance(distance, direction + 180)
            } else if (relaxFactor > 32) {
                logger.error("Location $this cannot be used to find another location.")
                return this
            }

            // only stop when we found the next location
            if (nextLocation.isInBrokerArea(brokerArea)) {
                logger.trace("Next location is $nextLocation")
                return nextLocation
            }
        }

    }

}

/**
 * Check whether [this] is in the given [brokerArea].
 */
fun Location.isInBrokerArea(brokerArea: List<Geofence>): Boolean {
    for (geofence in brokerArea) {
        if (geofence.contains(this)) {
            return true
        }
    }
    return false
}