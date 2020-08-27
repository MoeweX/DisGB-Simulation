package de.hasenburg.broker.simulation.main.simulation

import de.hasenburg.broker.simulation.main.Conf
import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.broker.simulation.main.stack.Message.*
import de.hasenburg.broker.simulation.main.stack.Stack
import org.apache.logging.log4j.LogManager
import java.lang.StringBuilder

private val logger = LogManager.getLogger()

/**
 * Converts and sorts the messages found in [stack] to a string comprising [CSVLine]s and a header.
 * This string can be directly written to a file, if needed.
 *
 * As this method also writes the [Conf.Permutation], it only works for worldcities data.
 */
fun preprocess(stack: Stack, permutation: Conf.Permutation): List<CSVLine> {
    val stackMessages = stack.getAllMessages()
    val csvLines = mutableListOf<CSVLine>()

    for (message in stackMessages) {
        csvLines.add(CSVLine.fromMessage(message, permutation))
    }

    csvLines.sort()

    return csvLines
}

/**
 * The type of the given message.
 * If the first character is a B, the message is a broker-broker message.
 * If the first character is a C, the message is a client-broker message.
 */
enum class Type {
    CLocationUpdate,
    CSubscriptionUpdate,
    CSubscriptionRemoval,
    CEventMatching,
    CEventDelivery,
    BLocationUpdate,
    BSubscriptionUpdate,
    BSubscriptionRemoval,
    BEventMatching,
    BEventDelivery
}

/**
 * Every CSV line denotes a received message.
 * As we are not interested in following message step by step, we do not save who is the sender of the message;
 * the only exception is the origin as it is used to uniquely identify messages belonging together.
 *
 * - Origin: <ClientId> (c1)
 * - OriginTick: <Tick> (2510)
 * - Type: <Type> (LocationUpdate)
 * - Processor: <BrokerId> or <ClientId> (c1 or b1); depends on the type of message
 * - ProcessorTick: <Tick> (2515)
 */
data class CSVLine(val origin: ClientId, val originTick: Tick, val type: Type, val processor: String,
                           val processorTick: Tick, val permutation: Conf.Permutation) : Comparable<CSVLine> {

    companion object {
        fun getCSVHeader(permutation: Conf.Permutation): String {
            return "origin;originTick;type;processor;processorTick;${permutation.csvHeader}\n"
        }

        fun fromMessage(message: Message, pm: Conf.Permutation): CSVLine {
            val o = message.origin.id // origin
            val ot = message.origin.tick // origin tick

            val p = message.us.id // processor id
            val pt = message.now // processor tick

            return when (message) {
                is CMessage.CLocationUpdate -> CSVLine(o, ot, Type.CLocationUpdate, p, pt, pm)
                is CMessage.CSubscriptionUpdate -> CSVLine(o, ot, Type.CSubscriptionUpdate, p, pt, pm)
                is CMessage.CSubscriptionRemoval -> CSVLine(o, ot, Type.CSubscriptionRemoval, p, pt, pm)
                is CMessage.CEventMatching -> CSVLine(o, ot, Type.CEventMatching, p, pt, pm)
                // CEventDelivery arrives at a client rather than a broker; now is overwritten in class to correct tick
                is CMessage.CEventDelivery -> CSVLine(o, ot, Type.CEventDelivery, message.subscriber.id.id, pt, pm)
                is BMessage.BLocationUpdate -> CSVLine(o, ot, Type.BLocationUpdate, p, pt, pm)
                is BMessage.BSubscriptionUpdate -> CSVLine(o, ot, Type.BSubscriptionUpdate, p, pt, pm)
                is BMessage.BSubscriptionRemoval -> CSVLine(o, ot, Type.BSubscriptionRemoval, p, pt, pm)
                is BMessage.BEventMatching -> CSVLine(o, ot, Type.BEventMatching, p, pt, pm)
                is BMessage.BEventDelivery -> CSVLine(o, ot, Type.BEventDelivery, p, pt, pm)
            }
        }
    }

    fun toCSVLine(): String {
        return "${origin.id};${originTick.ms};${type.name};${processor};${processorTick.ms};${permutation.asCSVLine()}\n"
    }

    override fun compareTo(other: CSVLine): Int {
        val c1 = originTick.compareTo(other.originTick)
        if (c1 == 0) {
            val c2 = origin.id.compareTo(other.origin.id)
            if (c2 == 0) {
                return processorTick.compareTo(other.processorTick)
            }
            return c2
        }
        return c1
    }

}