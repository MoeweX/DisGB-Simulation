package de.hasenburg.broker.simulation.main.stack

import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation
import org.apache.commons.math3.stat.descriptive.rank.Max
import org.apache.commons.math3.stat.descriptive.rank.Min
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile

class StorelessStatistics {

    companion object Constants {
        val headerCSV = "min;firstQuartile;median;thirdQuartile;max;mean;standardDeviation\n"
    }

    val min = Min()
    val firstQuartile = PSquarePercentile(25.0)
    val median = PSquarePercentile(50.0)
    val thirdQuartile = PSquarePercentile(75.0)
    val max = Max()

    val mean = Mean()
    val stdv = StandardDeviation()

    fun addValue(value: Double) {
        min.increment(value)
        firstQuartile.increment(value)
        median.increment(value)
        thirdQuartile.increment(value)
        max.increment(value)
        mean.increment(value)
        stdv.increment(value)
    }

    fun getResultCSV(): String {
        return "${min.result};${firstQuartile.result};${median.result};${thirdQuartile.result};" +
                "${max.result};${mean.result};${stdv.result}\n"
    }

}