package de.hasenburg.broker.simulation.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import de.hasenburg.broker.simulation.main.broker.BrokerType
import de.hasenburg.broker.simulation.main.misc.Tick
import org.apache.logging.log4j.LogManager
import java.io.File

private val logger = LogManager.getLogger()

class Conf(parser: ArgParser) {
    val worldCitiesFile by parser
        .storing("-i", "--input", help = "path to local file containing the worldcities data") { File(this) }
        .default(File("data/simulation_input/worldcities.csv"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("${value.absolutePath} does not exist")
            }
            if (!value.isFile) {
                throw InvalidArgumentException("${value.absolutePath} is not a file")
            }
        }

    val worldCitiesMinPopulation by parser
        .storing("--pop", help = "minimum population of a city for it to be included in simulation") {
            this.toIntOrNull() ?: 0
        }
        .default(5000)

    val outputDir by parser
        .storing("-d", "--dir", help = "path to dir in which results will be stored") { File(this) }
        .default(File("data/results/"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("${value.absolutePath} does not exist")
            }
            if (!value.isDirectory) {
                throw InvalidArgumentException("${value.absolutePath} is not a directory")
            }
        }

    val history by parser
        .storing("--history", help = "keep history? true or false") { this.toBoolean() }
        .default(true)

    val resultPrefix by parser
        .storing("-p", "--prefix", help = "string prefix used to name result files")
        .default("test")

    val experimentTime by parser
        .storing("-e", "--expTime", help = "Experiment time in seconds, e.g., 1000") { Tick(this.toInt() * 1000) }
        .default(Tick(1000000)) // 1000 seconds

    val brokerTypes by parser
        .storing("--brokers",
                help = "Broker Types to be used, e.g., BrokerFloodingEvents,BrokerFloodingSubscriptions") {
            this.split(",").map { BrokerType.valueOf(it) }
        }
        .default(listOf(BrokerType.BrokerFloodingEvents,
                BrokerType.BrokerFloodingSubscriptions,
                BrokerType.BrokerDisGBEvents,
                BrokerType.BrokerDisGBSubscriptions,
                BrokerType.BrokerDHT,
                BrokerType.BrokerGQPS,
                BrokerType.BrokerBG))

    val numberOfBrokers by parser
        .storing("--nBrokers", help = "number of brokers, e.g., 5,10,20") {
            this.split(",").map { it.toInt() }
        }
        .default(listOf(4))

    val numberOfClients by parser
        .storing("--nClients", help = "number of clients, e.g., 5,10,20") {
            this.split(",").map { it.toInt() }
        }
        .default(listOf(10))

    val numberOfTopics by parser
        .storing("--nTopics", help = "number of topics, e.g., 5,10,20") {
            this.split(",").map { it.toInt() }
        }
        .default(listOf(10))

    val eventGeofenceSize by parser
        .storing("--eGeofence", help = "event geofence size in degree, e.g., 1.0,10.0,50.0") {
            this.split(",").map { it.toDouble() }
        }
        .default(listOf(50.0))

    val subscriptionGeofenceSize by parser
        .storing("--sGeofence", help = "subscription geofence size in degree, e.g., 1.0,10.0,50.0") {
            this.split(",").map { it.toDouble() }
        }
        .default(listOf(50.0))

    val seed by parser
        .storing("--seed", help = "seed for randomization, e.g., 112358") { this.toInt() }
        .default(112358)

    val fieldSize by parser
        .storing("--fieldSize", help = "broker field size, e.g., 10") { this.toInt() }
        .default(10)

    /*****************************************************************
     * Permutations
     ****************************************************************/

    data class Permutation(val brokerType: BrokerType, val eventGeofenceSize: Double,
                           val subscriptionGeofenceSize: Double, val numberOfTopics: Int, val numberOfClients: Int,
                           val numberOfBrokers: Int) {

        val csvHeader: String = "numberOfBrokers;numberOfClients;numberOfTopics;subscriptionGeofenceSize;" +
                "eventGeofenceSize;brokerType"

        val pathSuffix = "$numberOfBrokers-$numberOfClients-$numberOfTopics-" +
                "$subscriptionGeofenceSize-$eventGeofenceSize-$brokerType"

        fun asCSVLine(): String {
            return pathSuffix.replace("-", ";")
        }


    }

    val permutations = {
        val result = mutableListOf<Permutation>()
        for (f in numberOfBrokers) {
            for (e in numberOfClients) {
                for (d in numberOfTopics) {
                    for (c in subscriptionGeofenceSize) {
                        for (b in eventGeofenceSize) {
                            for (a in brokerTypes) {
                                result.add(Permutation(a, b, c, d, e, f))
                            }
                        }
                    }
                }
            }
        }
        result.toList()
    }()

    init {
        logger.info(toString())
    }

    override fun toString(): String {
        return """
        Configuration(
            numberOfPermutations = ${permutations.size}
            worldCitiesFile = $worldCitiesFile
            outputDir = $outputDir
            history = $history
            resultPrefix = $resultPrefix
            experimentTime = $experimentTime
            brokerTypes = $brokerTypes
            numberOfBrokers = $numberOfBrokers
            numberOfClients = $numberOfClients
            numberOfTopics = $numberOfTopics
            eventGeofenceSize = $eventGeofenceSize
            subscriptionGeofenceSize = $subscriptionGeofenceSize
            seed = $seed
            fieldSize = $fieldSize
        )""".trimIndent()
    }

}