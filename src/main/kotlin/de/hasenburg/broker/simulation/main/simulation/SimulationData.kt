package de.hasenburg.broker.simulation.main.simulation

import de.hasenburg.broker.simulation.main.Conf
import de.hasenburg.broker.simulation.main.broker.*
import de.hasenburg.broker.simulation.main.misc.Topic
import de.hasenburg.broker.simulation.main.stack.Stack
import de.hasenburg.broker.simulation.main.writeToFile
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.broker.simulation.main.simulation.SimulationData.PlotLine.Function.*
import de.hasenburg.broker.simulation.main.simulation.SimulationData.PlotLine.Function.Broker
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.MessageCounter
import de.hasenburg.broker.simulation.main.stack.StorelessStatistics
import me.tongfei.progressbar.ProgressBar
import org.apache.logging.log4j.LogManager
import java.io.File
import java.lang.StringBuilder

private val logger = LogManager.getLogger()

data class SimulationData(val stack: Stack, val brokerDirectory: BrokerDirectory) {

    /*****************************************************************
     * Broker Plot Export
     ****************************************************************/

    /**
     * Abstract parent class for representing lines in CSV output
     * [brokerId] - id belonging to a broker
     * [lat] - latitude (pos of Broker, or center of BrokerField)
     * [lon] - longitude (pos of Broker, or center of BrokerField)
     * [function] - can be Broker (entry represents broker) or BrokerField (entry represents cover area of broker)
     */
    private abstract class PlotLine(val brokerId: String, val lat: Double, val lon: Double, val function: Function) :
            Comparable<PlotLine> {
        enum class Function { Broker, BrokerField }

        abstract fun toCSVLine(): String

        override fun compareTo(other: PlotLine): Int {
            val c1 = brokerId.compareTo(other.brokerId)
            if (c1 == 0) {
                return function.compareTo(function)
            }
            return c1
        }
    }

    /**
     * For DisGBEvents and DisGBSubscriptions
     */
    private class PlotLineGeneric(brokerId: String, lat: Double, lon: Double, function: Function,
                                  val area: Geofence? = null) :
            PlotLine(brokerId, lat, lon, function) {
        companion object {
            fun getHeader(): String {
                return "brokerId;lat;lon;function;boxMinX;boxMinY;boxMaxX;boxMaxY\n"
            }
        }

        override fun toCSVLine(): String {
            return "$brokerId;$lat;$lon;${function.name};" +
                    "${area?.boundingBox?.minX};${area?.boundingBox?.minY};" +
                    "${area?.boundingBox?.maxX};${area?.boundingBox?.maxY}\n"
        }
    }

    /**
     * For BrokerBG
     */
    private class PlotLineBrokerBG(brokerId: String, lat: Double, lon: Double, function: Function,
                                   val leaderId: BrokerId? = null, val area: Geofence? = null) :
            PlotLine(brokerId, lat, lon, function) {
        companion object {
            fun getHeader(): String {
                return "brokerId;lat;lon;function;leader;boxMinX;boxMinY;boxMaxX;boxMaxY\n"
            }
        }

        override fun toCSVLine(): String {
            return "$brokerId;$lat;$lon;${function.name};" +
                    "${leaderId?.id};" +
                    "${area?.boundingBox?.minX};${area?.boundingBox?.minY};" +
                    "${area?.boundingBox?.maxX};${area?.boundingBox?.maxY}\n"
        }
    }

    /**
     * For BrokerGQPS
     */
    private class PlotLineBrokerGQPS(brokerId: String, lat: Double, lon: Double, function: Function,
                                     val brokersInQuorum: List<BrokerId>? = null) :
            PlotLine(brokerId, lat, lon, function) {
        companion object {
            fun getHeader(): String {
                return "brokerId;lat;lon;function;brokersInQuorum\n"
            }
        }

        override fun toCSVLine(): String {
            return "$brokerId;$lat;$lon;${function.name};" +
                    "${brokersInQuorum?.map { it.id }?.reduce { acc, s -> "${acc},${s}" }?.dropLast(1)}\n"
        }
    }

    /**
     * For BrokerDHT
     */
    private class PlotLineBrokerDHT(brokerId: String, lat: Double, lon: Double, function: Function,
                                    val topics: List<Topic>? = null) :
            PlotLine(brokerId, lat, lon, function) {
        companion object {
            fun getHeader(): String {
                return "brokerId;lat;lon;function;topics\n"
            }
        }

        override fun toCSVLine(): String {
            return "$brokerId;$lat;$lon;${function.name};${topics?.map { it.topic }?.reduce { acc, s -> "$acc,$s" }}\n"
        }
    }

    /**
     * Creates a CSV string that comprises multiple [PlotLine] and a header.
     *
     *
     * [potentialBrokerLocations] - a list of all broker potential broker locations including the ones for which
     * the broker directory manages a broker
     */
    fun saveBrokerPlotCSV(outputFile: File, potentialBrokerLocations: List<Pair<Double, Double>>) {
        val randomBroker = brokerDirectory.brokers.values.random() // to test which strategy is used
        val bgManager = try { brokerDirectory.bgManager() } catch(e: IllegalStateException) { null }

        // remove the cloud broker if it exists
        val filteredLocations = brokerDirectory.brokerLocations
                .filter { (id, _) -> id != bgManager?.cloudBroker?.brokerId }

        require(potentialBrokerLocations.containsAll(filteredLocations.values.map { Pair(it.lat, it.lon) })) {
            "The given potentialBrokerLocations must contain the locations of brokers already managed by the directory."
        }

        val lines = mutableListOf<PlotLine>()
        val header: String

        when (randomBroker) {
            is BrokerGQPS -> {
                header = PlotLineBrokerGQPS.getHeader()
                filteredLocations.forEach { (brokerId, location) ->
                    lines.add(PlotLineBrokerGQPS(brokerId.id, location.lat, location.lon, PlotLine.Function.Broker,
                            brokersInQuorum = brokerDirectory.gridGQPS.getOtherBrokerIdsInSameColumn(brokerId)
                                    + brokerDirectory.gridGQPS.getOtherBrokerIdsInSameRow(brokerId)
                    ))
                }
            }
            is BrokerBG -> {
                header = PlotLineBrokerBG.getHeader()
                filteredLocations.forEach { (brokerId, location) ->
                    lines.add(PlotLineBrokerBG(brokerId.id, location.lat, location.lon, Broker,
                            leaderId = when {
                                bgManager == null -> null // null check for safety, should not be null
                                bgManager.isLeader(brokerId) -> brokerId // broker itself is leader
                                else -> bgManager.getLeader(brokerId) // otherwise, get leader of broker
                            }
                    ))
                }
                brokerDirectory.fieldAssignments.forEach { (geofence, brokerId) -> lines.add(PlotLineBrokerBG(brokerId.id,
                        geofence.center.lat, geofence.center.lon, BrokerField, area = geofence)) }
            }
            is BrokerDHT -> {
                header = PlotLineBrokerDHT.getHeader()

                val topics = mutableSetOf<Topic>()

                val clientMsg = stack.getAllMessages().filterIsInstance<Message.CMessage.CEventMatching>()

                clientMsg.forEach { topics.add(it.event.topic) }

                val brokerToTopics = mutableMapOf<BrokerId, MutableList<Topic>>()
                topics.forEach {
                    if (brokerToTopics[brokerDirectory.hashRouter.routeNode(it.topic)] == null) {
                        val topicList = mutableListOf<Topic>()
                        topicList.add(it)
                        brokerToTopics[brokerDirectory.hashRouter.routeNode(it.topic)] = topicList
                    } else {
                        brokerToTopics[brokerDirectory.hashRouter.routeNode(it.topic)]!!.add(it)
                    }
                }

                filteredLocations.forEach { (brokerId, location) ->
                    lines.add(PlotLineBrokerDHT(brokerId.id, location.lat, location.lon, Broker, brokerToTopics[brokerId]))
                }
            }
            else -> {
                header = PlotLineGeneric.getHeader()
                filteredLocations.forEach { (brokerId, location) ->
                    lines.add(PlotLineGeneric(brokerId.id, location.lat, location.lon, Broker))
                }
                brokerDirectory.fieldAssignments.forEach { (geofence, brokerId) -> lines.add(PlotLineGeneric(brokerId.id,
                        geofence.center.lat, geofence.center.lon, BrokerField, area = geofence)) }
            }
        }

        lines.sort()

        val resultString = header + lines.joinToString(separator = "") { it.toCSVLine() }
        resultString.writeToFile(outputFile)
    }

    /**
     * The [permutation] describes which world cities permutation the simulation is based on.
     */
    fun saveResults(basePath: String, permutation: Conf.Permutation) {
        logger.trace("Computing results")
        val csvLines = preprocess(stack, permutation)

        val observationsFile = File("$basePath.csv")
        observationsFile.parentFile.mkdirs()
        if (observationsFile.exists()) {
            logger.info("Overwrote file at ${observationsFile.absolutePath}")
        } else {
            logger.info("Created new file at ${observationsFile.absolutePath}")
        }

        val pb = ProgressBar("Saving results", csvLines.size.toLong())
        val bufferedWriter = observationsFile.bufferedWriter()
        bufferedWriter.write(CSVLine.getCSVHeader(permutation))
        for (csvLine in csvLines) {
            bufferedWriter.write(csvLine.toCSVLine())
            pb.step()
        }
        pb.close()
        bufferedWriter.flush()
        bufferedWriter.close()

        // write storeless statistics
        val ssFile = File(basePath + "_stats.csv")

        if (ssFile.exists()) {
            logger.info("Overwrote file at ${ssFile.absolutePath}")
        } else {
            logger.info("Created new file at ${ssFile.absolutePath}")
        }

        val text = StringBuilder()
        text.append("type;${StorelessStatistics.headerCSV}")
        text.append("edl;${stack.eventDeliveryLatencyStatistics.getResultCSV()}")
        text.append("sud;${stack.subscriptionUpdateDelayStatistics.getResultCSV()}")
        ssFile.writeText(text.toString())

        // write message counts
        val cFile = File(basePath + "_counts.csv")

        if (cFile.exists()) {
            logger.info("Overwrote file at ${cFile.absolutePath}")
        } else {
            logger.info("Created new file at ${cFile.absolutePath}")
        }

        text.clear()
        text.append(MessageCounter.headerCSV)
        text.append(stack.messageCounts.getResultCSV())
        cFile.writeText(text.toString())
    }
}