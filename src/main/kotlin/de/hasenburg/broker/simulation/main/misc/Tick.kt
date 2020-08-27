package de.hasenburg.broker.simulation.main.misc

import kotlin.random.Random

data class Tick(val ms: Int) : Comparable<Tick> {

    operator fun inc(): Tick {
        return Tick(ms + 1)
    }

    operator fun minus(other: Tick): Tick {
        return Tick(ms - other.ms)
    }

    operator fun minus(other: Int): Tick {
        return Tick(ms - other)
    }

    operator fun plus(other: Tick): Tick {
        return Tick(ms + other.ms)
    }

    operator fun plus(otherMs: Int) : Tick {
        return Tick(ms + otherMs)
    }

    override operator fun compareTo(other: Tick): Int {
        return ms.compareTo(other.ms)
    }

    /**
     * @return a Tick between this tick and [other], (exclusive).
     */
    fun pickBefore(other: Tick, random: Random) : Tick {
        return (Tick(random.nextInt(ms + 1, other.ms)))
    }

}