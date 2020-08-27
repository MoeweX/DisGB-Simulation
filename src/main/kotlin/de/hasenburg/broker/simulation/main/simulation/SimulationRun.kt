package de.hasenburg.broker.simulation.main.simulation

import de.hasenburg.broker.simulation.main.broker.Broker
import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.broker.BrokerDirectory
import de.hasenburg.broker.simulation.main.eh
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import me.tongfei.progressbar.ProgressBar
import org.apache.logging.log4j.LogManager
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

private val logger = LogManager.getLogger()

/**
 * Creates and starts a simulation with the given [simulationData].
 * -> stops when for more than 100.000 Ticks no message was processed
 *
 * Note: the [simulationData] is updated during the run.
 */
class SimulationRun(private val simulationData: SimulationData) {

    @ExperimentalTime
    val runtime = measureTime { run() }
    var lastProcessedTick: Tick? = null

    private fun run() = runBlocking(eh) {
        val stack = simulationData.stack
        val brokerDirectory = simulationData.brokerDirectory
        var currentTick = Tick(0)
        var ticksWithoutMessage = Tick(0)

        val pb = ProgressBar("Simulation", stack.maxTick.ms.toLong())

        while (ticksWithoutMessage <= Tick(100000)) {
            val messagesToProcess = stack.getMessages(currentTick)
            val rs = Channel<Message>(Channel.UNLIMITED)

            if (messagesToProcess.isNotEmpty()) {
                logger.debug("$currentTick: ${messagesToProcess.size} messages are received")
                // first messages from brokers, as these should not overwrite received client messages
                messagesToProcess.processMessagesOfType(BMessage.BLocationUpdate::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(BMessage.BSubscriptionUpdate::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(BMessage.BSubscriptionRemoval::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(BMessage.BEventMatching::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(BMessage.BEventDelivery::class.java, brokerDirectory, rs)

                // now the messages from clients
                messagesToProcess.processMessagesOfType(CMessage.CLocationUpdate::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(CMessage.CSubscriptionUpdate::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(CMessage.CSubscriptionRemoval::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(CMessage.CEventMatching::class.java, brokerDirectory, rs)
                messagesToProcess.processMessagesOfType(CMessage.CEventDelivery::class.java, brokerDirectory, rs)
                rs.close()

                // run through resultChannel and add messages to stack
                for (message in rs) {
                    stack.addMessage(message, currentTick)
                }

                // update storeless statistics
                stack.calculateStorelessStatistics(currentTick)

                // update progressbar and reset timer
                ticksWithoutMessage = Tick(0)
                pb.stepTo(currentTick.ms.toLong()) // only step when we actually processed something
                pb.maxHint(stack.maxTick.ms.toLong()) // there might be a message that arrives even later
            } else {
                logger.trace("$currentTick: no messages are received")
                ticksWithoutMessage++
            }
            currentTick++

            logger.trace("Finished $currentTick")
        }
        lastProcessedTick = currentTick - ticksWithoutMessage - 1
        pb.close()
        logger.info("Finished simulation, last processed Tick was $lastProcessedTick" +
                ", if larger than maxTick, it is most likely an event delivery. " +
                "There were ${stack.getAllMessages().size} messages.")

        brokerDirectory.validateBrokerStates()
    }

    fun logResults() {
        val resultString = simulationData.stack.toCSV()
        logger.info("\n" + resultString)
    }

    /**
     * For each [BrokerId] in [this], calls the appropriate processXXX method of the corresponding [Broker] (identified by
     * querying the [brokerDirectory] for all messages found in [this] that have the given [type].
     * The individual processXXX methods are supposed to send resulting messages for other [Broker]s to the result
     * channel [rs].
     */
    private suspend fun <X> Map<BrokerId, List<Message>>.processMessagesOfType(type: Class<X>,
                                                                               brokerDirectory: BrokerDirectory,
                                                                               rs: SendChannel<Message>) {
        coroutineScope {
            val coroutines = mutableListOf<Job>()

            for ((brokerId, messages) in this@processMessagesOfType) {
                val broker = brokerDirectory.getBroker(brokerId)
                // only keep messages of type
                val toProcess = messages.filter { it.javaClass == type }

                // let the brokers process their messages concurrently
                coroutines.add(launch {
                    for (message in toProcess) {
                        // remember, toProcess does only contain messages of the defined type
                        when (message) {
                            is CMessage.CLocationUpdate -> broker.processLocationUpdate(message, rs)
                            is CMessage.CSubscriptionUpdate -> broker.processSubscriptionUpdate(message, rs)
                            is CMessage.CSubscriptionRemoval -> broker.processSubscriptionRemoval(message, rs)
                            is CMessage.CEventMatching -> broker.processEventMatching(message, rs)
                            is CMessage.CEventDelivery -> broker.processEventDelivery(message)

                            is BMessage.BLocationUpdate -> broker.processLocationUpdate(message, rs)
                            is BMessage.BSubscriptionUpdate -> broker.processSubscriptionUpdate(message, rs)
                            is BMessage.BSubscriptionRemoval -> broker.processSubscriptionRemoval(message, rs)
                            is BMessage.BEventMatching -> broker.processEventMatching(message, rs)
                            is BMessage.BEventDelivery -> broker.processEventDelivery(message, rs)
                        }
                    }
                })
            }
            coroutines.joinAll()
        }
    }

}