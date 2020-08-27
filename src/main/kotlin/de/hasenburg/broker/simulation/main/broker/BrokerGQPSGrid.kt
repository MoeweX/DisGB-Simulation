package de.hasenburg.broker.simulation.main.broker

import kotlin.math.sqrt

class BrokerGQPSGrid(val brokerIds: List<BrokerId>) {

    data class GridIndex(val row: Int, val col: Int)

    // maximum row or col index
    val maxRC = sqrt(brokerIds.size.toDouble()) - 1

    init {
        require(maxRC.toInt() - maxRC == 0.0) {
            "The square root of the given numbers of brokers must be a whole number, not the case for ${brokerIds.size}."
        }
    }

    /*****************************************************************
     * Index operations
     ****************************************************************/

    fun getRow(index: Int): Int {
        require(index < brokerIds.size)
        return (index / (maxRC + 1)).toInt()
    }

    fun getCol(index: Int): Int {
        require(index < brokerIds.size)
        return index % (maxRC + 1).toInt()
    }

    /**
     * Returns the [GridIndex] at which the given [brokerId] is positioned.
     */
    fun getGridIndex(brokerId: BrokerId): GridIndex {
        val listIndex = brokerIds.indexOf(brokerId)
        check(listIndex >= 0) { "BrokerId $brokerId not stored in grid" }

        return GridIndex(getRow(listIndex), getCol(listIndex))
    }

    /**
     * Returns to which index in the broker list the given [gridIndex] maps.
     */
    fun getListIndex(gridIndex: GridIndex): Int {
        return (gridIndex.row * (maxRC + 1) + gridIndex.col).toInt()
    }

    /*****************************************************************
     * Is-in operations
     ****************************************************************/

    fun areInSameRow(brokerId1: BrokerId, brokerId2: BrokerId): Boolean {
        val index1 = getGridIndex(brokerId1)
        val index2 = getGridIndex(brokerId2)
        return index1.row == index2.row
    }

    fun areInSameColumn(brokerId1: BrokerId, brokerId2: BrokerId): Boolean {
        val index1 = getGridIndex(brokerId1)
        val index2 = getGridIndex(brokerId2)
        return index1.col == index2.col
    }

    /*****************************************************************
     * Retrieval operations
     ****************************************************************/

    /**
     * Returns all broker ids that are positioned in the same row as the given [brokerId], including the given one.
     */
    fun getBrokerIdsInSameRow(brokerId: BrokerId): List<BrokerId> {
        val row = getGridIndex(brokerId).row
        val cols = 0..(maxRC).toInt()
        return cols
            .map { GridIndex(row, it) } // create grid index
            .map { getListIndex(it) } // get list index
            .map { brokerIds[it] } // find brokerId

    }

    /**
     * Returns all other broker ids that are positioned in the same row as the given [brokerId], i.e., excluding [brokerId].
     */
    fun getOtherBrokerIdsInSameRow(brokerId: BrokerId): List<BrokerId> {
        return getBrokerIdsInSameRow(brokerId).filter { it != brokerId }
    }

    /**
     * Returns all broker ids that are positioned in the same column as the given [brokerId], including the given one.
     */
    fun getBrokerIdsInSameColumn(brokerId: BrokerId): List<BrokerId> {
        val rows = 0..(maxRC).toInt()
        val col = getGridIndex(brokerId).col
        return rows
            .map { GridIndex(it, col) } // create grid index
            .map { getListIndex(it) } // get list index
            .map { brokerIds[it] } // find brokerId
    }

    /**
     * Returns all other broker ids that are positioned in the same column as the given [brokerId], i.e., excluding [brokerId].
     */
    fun getOtherBrokerIdsInSameColumn(brokerId: BrokerId): List<BrokerId> {
        return getBrokerIdsInSameColumn(brokerId).filter { it != brokerId }
    }

}