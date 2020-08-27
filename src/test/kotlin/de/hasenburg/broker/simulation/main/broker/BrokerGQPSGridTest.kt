package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.generateRandomStrings
import org.junit.Test

import org.junit.Assert.*
import org.junit.Before
import org.junit.BeforeClass

class BrokerGQPSGridTest {

    private lateinit var grid9: BrokerGQPSGrid
    private lateinit var grid1000: BrokerGQPSGrid

    @Before
    fun setUpClass() {
        grid9 = BrokerGQPSGrid(generateRandomStrings("b", 1, 9).map { BrokerId(it) })
        grid1000 = BrokerGQPSGrid(generateRandomStrings("b", 50, 10000).map { BrokerId(it) })
    }

    @Test
    fun getMaxRCTest() {
        assertEquals(2.0, grid9.maxRC, 0.01)
        assertEquals(99.0, grid1000.maxRC, 0.01)
    }


    /*****************************************************************
     * Index operations
     ****************************************************************/

    @Test
    fun getRowTest() {
        // 25
        assertEquals(0, grid9.getRow(0))
        assertEquals(0, grid9.getRow(1))
        assertEquals(0, grid9.getRow(2))
        assertEquals(1, grid9.getRow(3))
        assertEquals(1, grid9.getRow(4))
        assertEquals(1, grid9.getRow(5))
        assertEquals(2, grid9.getRow(6))
        assertEquals(2, grid9.getRow(7))
        assertEquals(2, grid9.getRow(8))

        // 10000
        assertEquals(0, grid1000.getRow(0))
        assertEquals(0, grid1000.getRow(99))
        assertEquals(1, grid1000.getRow(100))
        assertEquals(1, grid1000.getRow(199))
        assertEquals(9, grid1000.getRow(900))
        assertEquals(9, grid1000.getRow(999))
        assertEquals(99, grid1000.getRow(9900))
        assertEquals(99, grid1000.getRow(9999))
    }

    @Test
    fun getColTest() {
        // 25
        assertEquals(0, grid9.getCol(0))
        assertEquals(1, grid9.getCol(1))
        assertEquals(2, grid9.getCol(2))
        assertEquals(0, grid9.getCol(3))
        assertEquals(1, grid9.getCol(4))
        assertEquals(2, grid9.getCol(5))
        assertEquals(0, grid9.getCol(6))
        assertEquals(1, grid9.getCol(7))
        assertEquals(2, grid9.getCol(8))

        // 10000
        assertEquals(0, grid1000.getCol(0))
        assertEquals(99, grid1000.getCol(99))
        assertEquals(0, grid1000.getCol(100))
        assertEquals(99, grid1000.getCol(199))
        assertEquals(0, grid1000.getCol(900))
        assertEquals(99, grid1000.getCol(999))
        assertEquals(0, grid1000.getCol(9900))
        assertEquals(99, grid1000.getCol(9999))
    }

    @Test
    fun getGridIndexTest() {
        assertEquals(BrokerGQPSGrid.GridIndex(0, 0), grid9.getGridIndex(grid9.brokerIds[0]))
        assertEquals(BrokerGQPSGrid.GridIndex(0, 2), grid9.getGridIndex(grid9.brokerIds[2]))
        assertEquals(BrokerGQPSGrid.GridIndex(1, 0), grid9.getGridIndex(grid9.brokerIds[3]))
        assertEquals(BrokerGQPSGrid.GridIndex(1, 1), grid9.getGridIndex(grid9.brokerIds[4]))
        assertEquals(BrokerGQPSGrid.GridIndex(2, 2), grid9.getGridIndex(grid9.brokerIds[8]))
    }

    @Test
    fun getListIndexTest() {
        assertEquals(0, grid9.getListIndex(BrokerGQPSGrid.GridIndex(0, 0)))
        assertEquals(2, grid9.getListIndex(BrokerGQPSGrid.GridIndex(0, 2)))
        assertEquals(3, grid9.getListIndex(BrokerGQPSGrid.GridIndex(1, 0)))
        assertEquals(4, grid9.getListIndex(BrokerGQPSGrid.GridIndex(1, 1)))
        assertEquals(8, grid9.getListIndex(BrokerGQPSGrid.GridIndex(2, 2)))

    }

    /*****************************************************************
     * Is-in operations
     ****************************************************************/

    @Test
    fun areInSameRowTest() {
        assertTrue(grid9.areInSameRow(grid9.brokerIds[0], grid9.brokerIds[1]))
        assertTrue(grid9.areInSameRow(grid9.brokerIds[0], grid9.brokerIds[2]))
        assertTrue(grid9.areInSameRow(grid9.brokerIds[1], grid9.brokerIds[2]))

        for (brokerId in grid9.brokerIds.subList(3, 9)) {
            assertFalse(grid9.areInSameRow(grid9.brokerIds[0], brokerId))
        }
    }

    @Test
    fun areInSameColumnTest() {
        assertTrue(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[3]))
        assertTrue(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[6]))
        assertTrue(grid9.areInSameColumn(grid9.brokerIds[3], grid9.brokerIds[6]))

        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[1]))
        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[2]))
        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[4]))
        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[5]))
        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[7]))
        assertFalse(grid9.areInSameColumn(grid9.brokerIds[0], grid9.brokerIds[8]))
    }


    /*****************************************************************
     * Retrieval operations
     ****************************************************************/

    @Test
    fun getBrokerIdsInSameRowTest() {
        val row1 = grid9.getBrokerIdsInSameRow(grid9.brokerIds[1])
        assertEquals(3, row1.size)
        assertTrue(row1.containsAll(grid9.brokerIds.subList(0, 3)))
        val row2 = grid9.getBrokerIdsInSameRow(grid9.brokerIds[3])
        assertEquals(3, row2.size)
        assertTrue(row2.containsAll(grid9.brokerIds.subList(3, 6)))
        val row3 = grid9.getBrokerIdsInSameRow(grid9.brokerIds[8])
        assertEquals(3, row3.size)
        assertTrue(row3.containsAll(grid9.brokerIds.subList(6, 9)))
    }


    @Test
    fun getBrokerIdsInSameColumnTest() {
        val row1 = grid9.getBrokerIdsInSameColumn(grid9.brokerIds[1])
        assertEquals(3, row1.size)
        assertTrue(row1.contains(grid9.brokerIds[1]))
        assertTrue(row1.contains(grid9.brokerIds[4]))
        assertTrue(row1.contains(grid9.brokerIds[7]))
        val row2 = grid9.getBrokerIdsInSameColumn(grid9.brokerIds[3])
        assertEquals(3, row2.size)
        assertTrue(row2.contains(grid9.brokerIds[0]))
        assertTrue(row2.contains(grid9.brokerIds[3]))
        assertTrue(row2.contains(grid9.brokerIds[6]))
        val row3 = grid9.getBrokerIdsInSameColumn(grid9.brokerIds[8])
        assertEquals(3, row3.size)
        assertTrue(row3.contains(grid9.brokerIds[2]))
        assertTrue(row3.contains(grid9.brokerIds[5]))
        assertTrue(row3.contains(grid9.brokerIds[8]))
    }
}