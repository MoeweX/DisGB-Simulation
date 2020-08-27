package de.hasenburg.broker.simulation.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import de.hasenburg.broker.simulation.main.broker.*
import de.hasenburg.broker.simulation.main.client.ClientMessageHolder
import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.misc.Topic
import de.hasenburg.broker.simulation.main.broker.BrokerDirectory
import de.hasenburg.broker.simulation.main.simulation.SimulationData
import de.hasenburg.broker.simulation.main.simulation.SimulationRun
import de.hasenburg.broker.simulation.main.stack.Stack
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.CoroutineExceptionHandler
import me.tongfei.progressbar.ProgressBar
import org.apache.logging.log4j.LogManager
import java.io.File
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.random.Random.Default.nextInt
import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime

private val logger = LogManager.getLogger()

@ExperimentalTime
fun main(args: Array<String>) {
    if (args.isEmpty()) {
        logger.info("No args provided, running test simulation")
        val sim = SimulationRun(prepareTestData()).also { it.logResults() }
        logger.info("The simulation ran for ${sim.runtime}")
        exitProcess(0)
    }

    logger.info("Running world cities simulation")
    val conf = mainBody { ArgParser(args).parseInto(::Conf) }
    val simulationData = prepareWorldCitiesData(conf)
    for ((i, data) in simulationData.withIndex()) {
        logger.info("Running simulation ${conf.permutations[i]}")

        val basePath = "${conf.outputDir}/${conf.resultPrefix}-${conf.permutations[i].pathSuffix}"
        val sim = SimulationRun(data)

        data.saveBrokerPlotCSV(File("${basePath}_brokers.csv"),
                parsedBrokerData!!.map { Pair(it.location.lat, it.location.lon) })
        data.saveResults(basePath, conf.permutations[i])

        logger.info("The simulation ran for ${sim.runtime}")
    }
    logger.info("World cities simulation completed.")

    exitProcess(0)
}

/**
 * Generates random test data not considering the configuration.
 */
fun prepareTestData(): SimulationData {
    val topics = generateRandomStrings("t-", 1, 3).map { Topic(it) }
    val brokers = generateRandomStrings("b-", 1, 2)
        .map { BrokerFloodingEvents(BrokerId(it), Location.random()) }
    val clients = generateRandomStrings("c-", 1, 5).map { ClientMessageHolder(ClientId(it)) }

    val brokerDirectory = BrokerDirectory(45, brokers)

    val messages = clients.map {
        it.getMessagesForWanderingClient(topics, Tick(100000), 150.0, 180.0, brokerDirectory)
    }.flatten()

    val stack = Stack(messages)
    logger.info("Prepared Random Test Data")
    return SimulationData(stack, brokerDirectory)
}

fun prepareWorldCitiesData(conf: Conf): List<SimulationData> {
    val data = mutableListOf<SimulationData>()
    for (permutation in conf.permutations) {
        val random = Random(conf.seed)
        val topics = generateRandomStrings("t-", 9, permutation.numberOfTopics, conf.seed).map { Topic(it) }

        val (brokers, clientNumbers) = getWorldCitiesBrokersAndClientNumbers(conf.worldCitiesFile,
                permutation.numberOfBrokers,
                conf.worldCitiesMinPopulation,
                permutation.brokerType,
                permutation.numberOfClients,
                conf.fieldSize, // ensures that each broker has a broker area
                conf.seed)

        val brokerDirectory = if (permutation.brokerType == BrokerType.BrokerBG) {
            BrokerDirectory(conf.fieldSize, brokers, clientNumbers)
        } else {
            BrokerDirectory(conf.fieldSize, brokers)
        }

        // generate clients for each broker area
        val pb1 = ProgressBar("Iter BAs", clientNumbers.size.toLong())
        val clients = mutableListOf<ClientMessageHolder>()
        for ((brokerId, clientNumber) in clientNumbers) {
            repeat(clientNumber) {
                val brokerAreas = brokerDirectory.getBrokerArea(brokerId)
                // TODO only needed as long above returns a list
                val brokerArea = brokerAreas[random.nextInt(brokerAreas.size)]

                val startLocation = Location.randomInGeofence(brokerArea, random)!!
                clients.add(ClientMessageHolder(ClientId("${brokerId.id}-$it"), startLocation))
            }
            pb1.step()
        }
        pb1.close()

        // generate messages for each client
        val pb2 = ProgressBar("Prep Msgs", clients.size.toLong())
        val messages = clients.map {
            it.getMessagesForWanderingClient(topics,
                    conf.experimentTime,
                    permutation.eventGeofenceSize,
                    permutation.subscriptionGeofenceSize,
                    brokerDirectory, random).also {
                pb2.step()
            }
        }.flatten()
        pb2.close()

        val stack = Stack(messages, conf.history)
        logger.info("Prepared WorldCities Data for $permutation")
        data.add(SimulationData(stack, brokerDirectory))
    }
    return data
}

/*****************************************************************
 * Utility
 ****************************************************************/

fun generateRandomStrings(prefix: String, suffixLength: Int, amount: Int, seed: Int = nextInt()): Set<String> {
    require(amount > 0)
    val chars: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    val random = Random(seed)

    val result = mutableSetOf<String>()

    do {
        val randomString = (1..suffixLength)
            .map { chars[random.nextInt(0, chars.size)] }
            .joinToString("")

        result.add(prefix + randomString) // set prevents duplicates
    } while (result.size < amount)

    return result
}

/**
 * Coroutine unknown exception error handler
 */
val eh = CoroutineExceptionHandler { _, e ->
    logger.fatal("Unknown Exception, shutting down", e)
    exitProcess(1)
}

fun String.writeToFile(file: File) {
    file.parentFile.mkdirs()
    if (file.exists()) {
        file.writeText(this)
        logger.info("Overwrote file at ${file.absolutePath}")
    } else {
        file.writeText(this)
        logger.info("Created new file at ${file.absolutePath}")
    }
}

/*****************************************************************
 * World Cities Helper TODO move to own file
 ****************************************************************/

private data class BrokerData(val id: BrokerId, val location: Location, val country: String, val population: Int)

private var parsedBrokerData: List<BrokerData>? = null

/**
 * Parses the given input file that must contain world cities data from https://simplemaps.com/data/world-cities.
 * Note: this file is only parsed once, so subsequent calls return the same data
 *
 * [type] - What kind of broker to use
 * [minDistance] - Minimum distance between brokers
 *
 * @return a List of prepared [Broker] implementations and a map that contains the ids of these brokers to the number
 * of clients that connect to every one.
 */
fun getWorldCitiesBrokersAndClientNumbers(inputFile: File, numberOfBrokers: Int, minPopulation: Int,
                                          type: BrokerType,
                                          numberOfClients: Int, minDistance: Int,
                                          seed: Int): Pair<List<Broker>, Map<BrokerId, Int>> {
    if (parsedBrokerData == null) {
        logger.debug("Reading in input file, as this is the first call.")
        parsedBrokerData = inputFile.readLines().drop(1).map { it.getBrokerData() }
            .groupBy { it.location }
            .map {
                // remove duplicates
                if (it.value.size > 1) {
                    logger.debug("${it.value.size} brokers are at ${it.key}, picking ${it.value[0]}")
                }
                it.value[0]
            }
            .filter { bd -> bd.population >= minPopulation }
            .shuffled(Random(seed))
    }
    val brokerData = mutableListOf<BrokerData>()

    // select brokers
    for (parsed in parsedBrokerData!!) {
        // exit
        if (brokerData.size == numberOfBrokers) {
            break
        }

        // ensure that new location is not closer to any chosen location than minDistance
        if (brokerData.none { it.location.distanceRadiansTo(parsed.location) < minDistance }) {
            brokerData.add(parsed)
        }
    }
    check(brokerData.size == numberOfBrokers) { "Only ${brokerData.size} brokers match minDistance $minDistance" }

    val brokers = brokerData.map {
        when (type) {
            BrokerType.BrokerDisGBEvents -> BrokerDisGBEvents(it.id, it.location)
            BrokerType.BrokerDisGBSubscriptions -> BrokerDisGBSubscriptions(it.id, it.location)
            BrokerType.BrokerFloodingEvents -> BrokerFloodingEvents(it.id, it.location)
            BrokerType.BrokerFloodingSubscriptions -> BrokerFloodingSubscriptions(it.id, it.location)
            BrokerType.BrokerDHT -> BrokerDHT(it.id, it.location)
            BrokerType.BrokerGQPS -> BrokerGQPS(it.id, it.location)
            BrokerType.BrokerBG -> BrokerBG(it.id, it.location)
        }
    }

    // calculate client share per broker based on population
    val totalPopulation = brokerData.sumBy { it.population }
    val multiplier = numberOfClients.toDouble() / totalPopulation

    val clientNumbers = brokerData.map { it.id to (it.population * multiplier).toInt() }.toMap()
    val totalClientNumber = clientNumbers.values.sum()
    val diff = (numberOfClients - totalClientNumber).absoluteValue

    check(diff / totalClientNumber < 0.01) { "Theoretical number of clients should not diverge more than 1% from real number of clients" }
    return Pair(brokers, clientNumbers)
}

/**
 * Might throw a [NumberFormatException].
 */
private fun String.getBrokerData(): BrokerData {
    val split = this.split("\",\"")
    check(split.size == 11) { "$this is not a valid world city row" }
    val clients = split[9].replace(".0", "").toIntOrNull() ?: 1
    val id = split[10].replace("\"", "")

    return BrokerData(BrokerId("${split[1]}-$id"),
            Location(split[2].toDouble(), split[3].toDouble()),
            split[4],
            clients)
}