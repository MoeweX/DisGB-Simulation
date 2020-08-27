package de.hasenburg.broker.simulation.main

import de.hasenburg.geobroker.commons.sleepNoLog
import kotlinx.coroutines.channels.Channel
import kotlin.random.Random

fun main() {

    val t = Test("hi")

    val channel = Channel<Test>(Channel.UNLIMITED)
    channel.offer(t)

    t.s2 = "Buh2"
    channel.offer(t)

    val a = channel.poll()!!.s2
    val b = channel.poll()!!.s2

    t.s2 = "Go!"
    channel.offer(t)

    val c = channel.poll()!!.s2

    print("$a $b $c \n")

    val r1 = Random(100)
    val ri1 = r1.nextInt()

    sleepNoLog(1000, 0)

    val r2 = Random(100)
    val ri2 = r2.nextInt()

    print("$ri1 $ri2\n")

    val map1 = mutableMapOf(1 to 1, 2 to 2)
    map1.toMutableMap()[3] = 3
    val map2 = map1.toMutableMap()
    map2[3] = 3
    println(map1)
    println(map2)

}

data class Test(val s: String) {
    var s2 = "Buh"
}