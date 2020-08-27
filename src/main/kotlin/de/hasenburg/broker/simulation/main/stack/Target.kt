package de.hasenburg.broker.simulation.main.stack

import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Tick

data class CTarget(val id: ClientId, val tick: Tick): Comparable<CTarget> {

    override fun compareTo(other: CTarget): Int {
        val c1 = id.id.compareTo(other.id.id)
        if (c1 == 0) {
            return tick.compareTo(other.tick)
        }
        return c1
    }

}

data class BTarget(val id: BrokerId, val tick: Tick)