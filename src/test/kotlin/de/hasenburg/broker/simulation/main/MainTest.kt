package de.hasenburg.broker.simulation.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import de.hasenburg.broker.simulation.main.broker.BrokerType
import de.hasenburg.broker.simulation.main.simulation.SimulationRun
import org.apache.logging.log4j.LogManager
import org.junit.Test

import org.junit.Assert.*

private val logger = LogManager.getLogger()

class MainTest {

    /**
     * Tests whether [prepareWorldCitiesData] generates always the same.
     */
    @Test
    fun prepareWorldCitiesDataTest() {
        val conf = mainBody { ArgParser(arrayOf()).parseInto(::Conf) }

        val datas1 = prepareWorldCitiesData(conf)
        val datas2 = prepareWorldCitiesData(conf)

        // check size
        assertEquals(datas1.size, datas2.size)

        // check content
        repeat(datas1.size) {
            val data1 = datas1[it]
            val data2 = datas2[it]

            // broker directory
            assertEquals(data1.brokerDirectory, data2.brokerDirectory)

            // stack
            assertEquals(data1.stack, data2.stack)
            logger.info(data1.stack)
            logger.info(data2.stack)

            logger.info("SimulationData for ${conf.permutations[it]} are equal")
        }

        // check stacks of other positions
        repeat(datas1.size) {
            val data1 = datas1[0]
            val data2 = datas2[it]

            // stack
            assertEquals(data1.stack, data2.stack)

            logger.info("SimulationData for ${conf.permutations[it]} are equal")
        }
    }

    /**
     * Tests whether [prepareWorldCitiesData] generates always the same stack, even if broker types have changed.
     */
    @Test
    fun prepareWorldCitiesDataTest2() {
        val conf1 = mainBody {
            ArgParser(arrayOf("--brokers",
                    "BrokerFloodingEvents,BrokerFloodingSubscriptions")).parseInto(::Conf)
        }
        val conf2 = mainBody {
            ArgParser(arrayOf("--brokers",
                    "BrokerDisGBEvents,BrokerDisGBSubscriptions")).parseInto(::Conf)
        }

        val datas1 = prepareWorldCitiesData(conf1)
        val datas2 = prepareWorldCitiesData(conf2)

        // check size
        assertEquals(datas1.size, datas2.size)

        // check content
        repeat(datas1.size) {
            val data1 = datas1[it]
            val data2 = datas2[it]

            // stack
            assertEquals(data1.stack, data2.stack)

            logger.info("Stacks for ${conf1.permutations[it]} and ${conf2.permutations[it]} are equal")
        }

        // check with other positions
        repeat(datas1.size) {
            val data1 = datas1[0]
            val data2 = datas2[it]

            // stack
            assertEquals(data1.stack, data2.stack)

            logger.info("Stacks for ${conf1.permutations[it]} and ${conf2.permutations[it]} are equal")
        }
    }

    /**
     * Checks whether multiple executions of the same prepared world cities data lead to the same result.
     */
    @Test
    fun runWorldCitiesDataTest() {
        val conf = mainBody { ArgParser(arrayOf()).parseInto(::Conf) }

        val datas1 = prepareWorldCitiesData(conf)
        val datas2 = prepareWorldCitiesData(conf)

        repeat(datas1.size) {
            val data1 = datas1[it]
            val data2 = datas2[it]

            val sim1 = SimulationRun(data1)
            assertNotEquals(data1, data2)

            val sim2 = SimulationRun(data2)
            assertEquals(data1, data2)
            assertEquals(sim1.lastProcessedTick, sim2.lastProcessedTick)
        }

    }

}