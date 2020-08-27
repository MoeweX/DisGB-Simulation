package de.hasenburg.broker.simulation.main.broker

import com.github.jaskey.consistenthash.ConsistentHashRouter
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.misc.Topic
import de.hasenburg.broker.simulation.main.stack.BTarget
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import kotlin.math.ceil
import kotlin.math.roundToInt

private val logger = LogManager.getLogger()

/**
 * @param fieldSize - size of each squared field
 */
class BrokerDirectory(fieldSize: Int, brokerList: List<Broker>, clientNumbers: Map<BrokerId, Int>? = null) {

    companion object Constants {
        const val minLat: Int = -90
        const val maxLat: Int = 90
        const val minLon: Int = -180
        const val maxLon: Int = 180

        /**
         * Derived from 2016 IPlane data, max latency between two brokers is ~475ms (22500km)
         */
        const val msPerKm = 0.021048134571484346
    }

    init {
        // wire brokers
        for (broker in brokerList) {
            broker.wireBrokerDirectory(this)
        }
    }

    var brokers = brokerList.map { it.brokerId to it }.toMap()
    var brokerLocations = brokerList.map { it.brokerId to it.location }.toMap()

    /**
     * Stores which broker is closest to each field.
     */
    val fieldAssignments = Init.calculateFields(fieldSize)
        .map { it to Init.findBrokerClosestToLocation(it.center, brokerLocations) }
        .toMap()


    private val brokerAreaBuffer = fieldAssignments.let { assignments ->
        val ids = assignments.values.toSet()
        val resultMap = mutableMapOf<BrokerId, List<Geofence>>()
        for (id in ids) {
            val areas = assignments.filter { it.value == id }.map { it.key }.toList()
            resultMap[id] = areas
        }
        resultMap.toMap()
    }

    // report brokers without a broker area
    init {
        val brokerIds = brokers.keys
        val brokersWithFields = fieldAssignments.values
        val withoutBA = brokerIds.filter { !brokersWithFields.contains(it) }
        check(withoutBA.isEmpty()) { "${withoutBA.size} brokers do not have their own broker area: $withoutBA" }
    }

    object Init {

        /**
         * Calculates a set of square and neighboring [Geofence]s, the sides of each square are [fieldSize] degree.
         *
         * Used on initialization of a new [BrokerDirectory].
         */
        fun calculateFields(fieldSize: Int): List<Geofence> {
            require(maxLat % fieldSize == 0) { "The maximum latitude module fieldSize must be 0" }
            require(maxLon % fieldSize == 0) { "The maximum longitude module fieldSize must be 0" }

            val fields = mutableListOf<Geofence>()

            for (lat in minLat until maxLat step fieldSize) {
                for (lon in minLon until maxLon step fieldSize) {
                    val small = 0.0000000000001 // to prevent fields "touching"
                    val field = Geofence.rectangle(
                            Location(lat.toDouble(), lon.toDouble()),
                            Location((lat + fieldSize - small), (lon + fieldSize - small)))
                    fields.add(field)
                }
            }

            return fields
        }

        /**
         * Calculates which [Broker] is closest (in km) to the [targetLocation], returns the corresponding [BrokerId].
         *
         * Used on initialization of a new [BrokerDirectory].
         */
        fun findBrokerClosestToLocation(targetLocation: Location, brokerLocations: Map<BrokerId, Location>): BrokerId {
            var assignedBroker = Pair(BrokerId("none"), Double.MAX_VALUE) // brokerId -> distance to field

            for ((brokerId, location) in brokerLocations) {
                val distance = targetLocation.distanceKmTo(location)
                if (distance < assignedBroker.second) {
                    assignedBroker = Pair(brokerId, distance)
                }
            }

            check(assignedBroker.first.id != "none")
            return assignedBroker.first
        }

    }


    /*****************************************************************
     * Broker Structures
     ****************************************************************/

    /**
     * The manager of the hash ring needed for consisting hashing brokers such as [BrokerDHT].
     */
    val hashRouter = ConsistentHashRouter(brokers.keys, 10)

    /**
     * The manager of the broker GQPS grid needed for the [BrokerGQPS].
     */
    val gridGQPS = BrokerGQPSGrid(brokers.keys.toList())

    /**
     * The manager of the broadcast groups needed for the [BrokerBG].
     * [clientNumbers] is used to determine the leader.
     */
    private val _bgManager = clientNumbers
        ?.let { BrokerBGManager(this, it) }

    fun bgManager() = _bgManager ?: error {
        "BGManager not initialized, did you supply clientNumbers to broker directory?"
    }

    /*****************************************************************
     * API
     ****************************************************************/

    /**
     * Validates that the broker internal fields are as expected for the brokers, that have such fields.
     * In case of an error, logs the cause; otherwise does nothing.
     */
    fun validateBrokerStates() {
        for (broker in brokers.values) {
            if (broker is BrokerGQPS) {
                if (broker.deliveredEvents.isNotEmpty()) {
                    logger.error("Delivered events of ${broker.brokerId} is not empty, is ${broker.deliveredEvents}")
                }
            }
        }
    }

    fun getBroker(brokerId: BrokerId): Broker {
        return brokers[brokerId] ?: error("Broker $brokerId does not exist.")
    }

    fun getBrokerLocation(brokerId: BrokerId): Location {
        return brokerLocations[brokerId] ?: error("There is not location for broker $brokerId.")
    }

    fun getLatencyBetweenBrokers(brokerId1: BrokerId, brokerId2: BrokerId): Int {
        val l1 = getBrokerLocation(brokerId1)
        val l2 = getBrokerLocation(brokerId2)
        return ceil(l1.distanceKmTo(l2) * msPerKm).roundToInt()
    }

    /**
     * Returns the broker area for the given broker. At the moment, the broker area comprises multiple neighbooring and
     * square [Geofence]s.
     * TODO return a single Geofence that comprises the whole area
     */
    fun getBrokerArea(brokerId: BrokerId): List<Geofence> {
        return brokerAreaBuffer[brokerId] ?: error("$brokerId has no broker area")
    }

    fun getLocalBroker(location: Location): BrokerId {
        for (assignment in fieldAssignments) {
            if (assignment.key.contains(location)) {
                return assignment.value
            }
        }
        error("Location $location does not have a local broker.")
    }

    /**
     * Returns the [BrokerId] of every [Broker] that has a [BrokerArea] intersecting with the given [geofence].
     *
     * TODO it might be wise merge all fields of the same broker into one broker area beforehand
     */
    private fun getAffectedBrokers(geofence: Geofence): Set<BrokerId> {
        return fieldAssignments.filter { it.key.intersects(geofence) }.map { it.value }.toSet()
    }

    /**
     * Calculates which brokers have been affected by [oldGeofence] and not by [newGeofence].
     */
    private fun getFormerlyAffectedBrokers(oldGeofence: Geofence, newGeofence: Geofence): List<BrokerId> {
        val oldAffected = getAffectedBrokers(oldGeofence)
        val newAffected = getAffectedBrokers(newGeofence)

        return oldAffected.filter { !newAffected.contains(it) }
    }

    /*****************************************************************
     * Latency helper
     ****************************************************************/

    /**
     * Gets the latency to every other broker from [origin].
     */
    private fun getLatencyToOtherBrokers(origin: BrokerId): Map<BrokerId, Int> {
        return brokerLocations
            .filter { it.key != origin } // everyone but origin
            .map { it.key to getLatencyBetweenBrokers(origin, it.key) }
            .toMap()
    }

    /**
     * Gets the latency to every other broker from [origin] that is affected by [geofence].
     */
    private fun getLatencyToOtherAffectedBrokers(origin: BrokerId, geofence: Geofence): Map<BrokerId, Int> {
        val affectedBrokers = getAffectedBrokers(geofence)
        return affectedBrokers
            .filter { it != origin } // everyone but origin
            .map { it to getLatencyBetweenBrokers(origin, it) }
            .toMap()
    }

    /**
     * Gets the latency to every other broker from [origin] that was affected by [oldGeofence] but is not affected by
     * [newGeofence].
     */
    private fun getLatencyToOtherFormerlyAffectedBrokers(origin: BrokerId, oldGeofence: Geofence,
                                                         newGeofence: Geofence): Map<BrokerId, Int> {
        val formerlyAffectedBrokers = getFormerlyAffectedBrokers(oldGeofence, newGeofence)
        return formerlyAffectedBrokers
            .filter { it != origin } // everyone but origin
            .map { it to getLatencyBetweenBrokers(origin, it) }
            .toMap()
    }

    /*****************************************************************
     * BTarget helper
     ****************************************************************/

    /**
     * Gets a BTarget to every other broker.
     * This means, if a message was send [now] from [origin], it would arrive at every returned BTarget as defined.
     */
    fun getBTargetsForOtherBrokers(origin: BrokerId, now: Tick): List<BTarget> {
        return getLatencyToOtherBrokers(origin)
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /**
     * Gets a BTarget to every other broker that is affected by [geofence].
     * This means, if a message was send [now] from [origin], it would arrive at every returned BTarget as defined.
     */
    fun getBTargetsForOtherAffectedBrokers(origin: BrokerId, now: Tick, geofence: Geofence): List<BTarget> {
        return getLatencyToOtherAffectedBrokers(origin, geofence)
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /**
     * Gets a BTarget to every other broker that was affected by [oldGeofence] but is not affected by
     * [newGeofence].
     * This means, if a message was send [now] from [origin], it would arrive at every returned BTarget as defined.
     */
    fun getBTargetsForOtherFormerlyAffectedBrokers(origin: BrokerId, now: Tick, oldGeofence: Geofence,
                                                   newGeofence: Geofence): List<BTarget> {
        return getLatencyToOtherFormerlyAffectedBrokers(origin, oldGeofence, newGeofence)
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /**
     * Gets the BTarget to the broker matching the hash of [topic].
     * This means, if a message was send [now] from [origin], it would arrive at the returned BTarget as defined.
     */
    fun getBTargetUsingHashing(origin: BrokerId, now: Tick, topic: Topic): BTarget {
        val brokerId = hashRouter.routeNode(topic.topic)
        val latency = getLatencyBetweenBrokers(origin, brokerId)
        return BTarget(brokerId, now + latency)
    }

    /**
     * Gets the BTarget to every other broker that is also placed in the same row as [origin].
     * This means, if a message was send [now] from [origin], it would arrive at the returned BTarget as defined.
     */
    fun getBTargetsForOtherBrokersInRow(origin: BrokerId, now: Tick): List<BTarget> {
        return gridGQPS.getOtherBrokerIdsInSameRow(origin)
            .map { it to getLatencyBetweenBrokers(origin, it) }
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /**
     * Gets the BTarget to every other broker that is also placed in the same column as [origin].
     * This means, if a message was send [now] from [origin], it would arrive at the returned BTarget as defined.
     */
    fun getBTargetsForOtherBrokersInColumn(origin: BrokerId, now: Tick): List<BTarget> {
        return gridGQPS.getOtherBrokerIdsInSameColumn(origin)
            .map { it to getLatencyBetweenBrokers(origin, it) }
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /**
     * Gets the BTarget to every other broker that is also placed in the same row and column as [origin].
     * This means, if a message was send [now] from [origin], it would arrive at the returned BTarget as defined.
     */
    fun getBTargetsForOtherBrokersInRowAndColumn(origin: BrokerId, now: Tick): List<BTarget> {
        val inRow = gridGQPS.getOtherBrokerIdsInSameRow(origin)
        val inColumn = gridGQPS.getOtherBrokerIdsInSameColumn(origin)

        return setOf(inRow, inColumn)
            .flatten() // set of broker ids rather than lists
            .map { it to getLatencyBetweenBrokers(origin, it) }
            .map { (brokerId, latency) -> BTarget(brokerId, now + latency) }
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BrokerDirectory

        if (brokers != other.brokers) return false
        if (brokerLocations != other.brokerLocations) return false
        if (fieldAssignments != other.fieldAssignments) return false

        return true
    }

    override fun hashCode(): Int {
        var result = brokers.hashCode()
        result = 31 * result + brokerLocations.hashCode()
        result = 31 * result + fieldAssignments.hashCode()
        return result
    }

    override fun toString(): String {
        return "BrokerDirectory(brokerLocations=$brokerLocations)"
    }

}

