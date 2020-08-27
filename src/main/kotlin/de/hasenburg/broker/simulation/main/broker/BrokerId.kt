package de.hasenburg.broker.simulation.main.broker

import com.github.jaskey.consistenthash.*

/**
 * Extends Node so that the broker id can be used for consistent hashing, see [ConsistentHashRouter].
 */
data class BrokerId(val id: String) : Node {

    override fun getKey(): String {
        return id
    }

}