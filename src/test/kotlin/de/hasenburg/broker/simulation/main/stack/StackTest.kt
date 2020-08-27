package de.hasenburg.broker.simulation.main.stack

import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Tick
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Before
import org.junit.Test
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*

private val logger = LogManager.getLogger()

class StackTest {

    @Before
    fun setUp() {
    }

    @After
    fun tearDown() {
    }

    @Test
    fun validateStackCreationTest() {
        val goodList = genCorrectList()
        Stack(goodList)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateClientBrokerAreaTestFail() {
        val badList = genCorrectList()
        badList.add(CLocationUpdate(Location(52.0,52.0),
                CTarget(ClientId("c1"), Tick(10)),
                BTarget(BrokerId("b2"), Tick(110)))) // Broker different
        Stack(badList)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateCTargetTestFail() {
        val badList = genCorrectList()
        badList.add(CLocationUpdate(Location(52.0,52.0),
                CTarget(ClientId("c1"), Tick(0)), // CTarget not unique
                BTarget(BrokerId("b1"), Tick(110))))
        Stack(badList)
    }

    fun genCorrectList(): MutableList<Message> {
        val list = mutableListOf<Message>()
        list.add(CLocationUpdate(Location(50.0,50.0),
                CTarget(ClientId("c1"), Tick(0)),
                BTarget(BrokerId("b1"), Tick(100))))
        list.add(CLocationUpdate(Location(51.0,51.0),
                CTarget(ClientId("c1"), Tick(5)),
                BTarget(BrokerId("b1"), Tick(105))))
        return list
    }
}
