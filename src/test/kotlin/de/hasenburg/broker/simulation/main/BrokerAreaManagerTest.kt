package de.hasenburg.broker.simulation.main

import de.hasenburg.broker.simulation.main.broker.BrokerDirectory.*
import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.junit.After
import org.junit.Before
import org.junit.Test

import org.junit.Assert.*

private val logger = LogManager.getLogger()

class BrokerAreaManagerTest {

    @Before
    fun setUp() {
    }

    @After
    fun tearDown() {
    }

    @Test
    fun calculateFieldsTest() {
        val fieldSizes = listOf(5, 10, 45, 90) // 1 and 2 takes very long to validate
        for (fieldSize in fieldSizes) {
            validateFields(Init.calculateFields(fieldSize), fieldSize)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun calculateFieldsTestFail() {
        Init.calculateFields(4)
    }

    @Test
    fun assignFieldToBrokerTest() {
        val fields = Init.calculateFields(45)
        val brokers = mapOf(
                BrokerId("broker1") to Location(45.0, 45.0),
                BrokerId("broker2") to Location(-45.0, -45.0))

        val assignments = fields.map { Init.findBrokerClosestToLocation(it.center, brokers) }

        // both brokers should have the same amount of fields
        assertEquals(assignments.filter { it == BrokerId("broker1") }.size,
                assignments.filter { it == BrokerId("broker2") }.size)

        logger.info("Both brokers have ${assignments.size / 2} fields assigned.")
    }

    @Test
    fun randomizedBrokersTest() {
        val fields = Init.calculateFields(10)
        val brokers = (1..100)
            .map { BrokerId("Broker-$it") to Location.random() }.toMap()

        val assignments = fields
            .map { it to Init.findBrokerClosestToLocation(it.center, brokers) }
        
        for (assignment in assignments) {
            var resultBrokerId = BrokerId("")
            var minDistance = Double.MAX_VALUE
            for ((brokerId, bLocation) in brokers) {
                val distance = bLocation.distanceKmTo(assignment.first.center)
                if (distance < minDistance) {
                    minDistance = distance
                    resultBrokerId = brokerId
                    logger.trace("$brokerId has a distance of ${minDistance}km")
                }
            }
            assertEquals(assignment.second, resultBrokerId)
            logger.debug("Broker $resultBrokerId is closest to ${assignment.first}")
        }
        logger.info("BrokerArea assignments calculated correctly.")
    }

    private fun validateFields(fields: List<Geofence>, fieldSize: Int) {
        for (field in fields) {
            // no field intersects with another field
            val intersections = fields.filter { it != field }.filter { it.intersects(field) }
            assertTrue(intersections.isEmpty())
        }

        // number of fields is correct
        val expectedFields = (360 / fieldSize) * (180 / fieldSize)
        assertEquals(expectedFields, fields.size)

        logger.info("Validated brokers fields for field size $fieldSize, there are ${fields.size} fields.")
    }

}