package de.hasenburg.broker.simulation.main.stack

import de.hasenburg.broker.simulation.main.broker.BrokerType
import org.apache.logging.log4j.LogManager
import java.lang.StringBuilder

private val logger = LogManager.getLogger()

class MessageCounter {

    companion object Constants {
        val headerCSV = "type;count\n"
    }

    private val counts = mutableMapOf<String, Int>()

    fun countMessage(message: Message) {
        val key = message::class.java.simpleName
        counts[key] = counts.getOrPut(key) { 0 }.inc()
    }

    fun getResultCSV(): String {
        val result = StringBuilder()
        for ((type, count) in counts) {
            result.append("$type;$count\n")
        }
        return result.toString()
    }
}

