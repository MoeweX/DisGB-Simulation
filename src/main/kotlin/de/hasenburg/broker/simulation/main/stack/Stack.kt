package de.hasenburg.broker.simulation.main.stack

import de.hasenburg.broker.simulation.main.broker.Broker
import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.broker.simulation.main.misc.Tick
import org.apache.logging.log4j.LogManager
import java.lang.StringBuilder

private val logger = LogManager.getLogger()

/**
 * The [Stack] stores the messages that have been or will be processed by brokers or received by clients.
 * @param keepHistory - if true, the stack will preserve all messages; if false, the stack will remove all past messages
 *                      after it calculated the storeless statistics
 */
class Stack(messages: List<Message>, private val keepHistory: Boolean = true) {

    private val tickMap = mutableMapOf<Tick, StackEntry>()
    val eventDeliveryLatencyStatistics = StorelessStatistics()
    val subscriptionUpdateDelayStatistics = StorelessStatistics()
    val messageCounts = MessageCounter()

    var maxTick: Tick = Tick(0)
        private set

    init {
        require(validateClientBrokerArea(messages)) { "Clients can only have one broker location" }
        require(validateCTarget(messages)) { "CTargets of messages must be unique" }
        messages.forEach { addMessage(it, Tick(0)) }
        logger.debug("Added ${messages.size} client messages to stack")
    }

    private fun validateClientBrokerArea(messages: List<Message>): Boolean {
        val clientMessageMap = messages.groupBy { it.origin.id.id }
        for ((_, clientMessages) in clientMessageMap) {
            val initialLocalBroker = clientMessages[0].us.id
            val result = clientMessages.drop(1).all { it.us.id == initialLocalBroker }
            if (!result) {
                return false
            }
        }
        return true
    }

    private fun validateCTarget(messages: List<Message>): Boolean {
        val setCTarget = messages.map { it.origin }.toSet()
        return messages.size == setCTarget.size
    }

    fun addMessage(message: Message, currentTick: Tick) {
        require(currentTick < message.now) { "Cannot add past messages to stack: $message, we are at $currentTick" }

        val stack = tickMap.getOrDefault(message.now, StackEntry()).also { it.addMessage(message) }
        tickMap[message.now] = stack
        // update maxTick
        if (maxTick < message.now) {
            maxTick = message.now
        }

        logger.trace("Added $message to stack")
    }

    /**
     * Returns all messages that should be processed at the given tick for each broker that does the processing.
     */
    fun getMessages(tick: Tick): Map<BrokerId, List<Message>> {
        return tickMap[tick]?.messages ?: emptyMap()
    }

    fun getAllMessages(): List<Message> {
        return tickMap.values.map { it.messages.values.flatten() }.flatten()
    }

    /**
     * Updates the [StorelessStatistics] with data from the given tick.
     * This method should be called after each processed tick.
     *
     * The event delivery latency is the time the subscriber received a message - time the publisher published a message
     * The subscription update delay is the time a broker received the subscription update - time the first broker
     * send the update (this is the time the publisher created the subscription + client latency)
     */
    fun calculateStorelessStatistics(currentTick: Tick) {
        val messages = if (keepHistory) {
            tickMap[currentTick]?.allMessages() ?: emptyList()
        } else {
            tickMap.remove(currentTick)?.allMessages() ?: emptyList()
        }

        for (message in messages) {
            messageCounts.countMessage(message)
            if (message is Message.CMessage.CEventDelivery) {
                val latency = message.now - message.origin.tick
                eventDeliveryLatencyStatistics.addValue(latency.ms.toDouble())
            } else if (message is Message.BMessage.BSubscriptionUpdate) {
                val delay = message.now - message.origin.tick + Broker.clientLatency
                subscriptionUpdateDelayStatistics.addValue(delay.ms.toDouble())
            }
        }

    }

    fun toCSV(): String {
        val csv = StringBuilder()
        csv.append("\nReceiveTick;Message\n")
        for ((tick, entry) in tickMap.toSortedMap()) {
            for (message in entry.messages.values.flatten()) {
                csv.append("$tick;$message\n")
            }
        }
        return csv.toString()
    }

    override fun toString(): String {
        return "Stack (${tickMap.size} ticks have a message)"
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Stack) return false

        if (tickMap != other.tickMap) return false

        return true
    }

    override fun hashCode(): Int {
        return tickMap.hashCode()
    }

}

private class StackEntry {
    val messages = mutableMapOf<BrokerId, MutableList<Message>>()

    fun addMessage(message: Message) {
        val brokerId = message.us
        val list = messages.getOrDefault(brokerId, mutableListOf()).also { it.add(message) }
        list.sort() // to ensure determinism
        messages[brokerId] = list
    }

    fun allMessages() : List<Message> {
        return messages.values.flatten()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StackEntry) return false

        if (messages != other.messages) return false

        return true
    }

    override fun hashCode(): Int {
        return messages.hashCode()
    }

}

