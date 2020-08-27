package de.hasenburg.broker.simulation.main.broker

import de.hasenburg.broker.simulation.main.broker.BrokerBG.*
import de.hasenburg.broker.simulation.main.stack.Message
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager

private val logger = LogManager.getLogger()

/**
 * BroadcastGroup manager for [BrokerBG].
 *
 * The Cloud broker is located at in Ashburn (Loudoun County), Virgia, USA.
 *
 * According to https://biz.loudoun.gov/key-business-sectors/data-centers/, up to 70% of the
 * internet traffic is routed through this region.
 *
 * Adds a "Cloud" broker to the [brokerDirectory], as it is needed by [BrokerBG]. Uses [clientNumbers] for the group
 * formation.
 */
class BrokerBGManager(val brokerDirectory: BrokerDirectory, val clientNumbers: Map<BrokerId, Int>) {

    companion object Constants {
        const val latencyThreshold = 100 // ms (should lead to around 10 broadcast groups according to BG paper)
    }

    /*****************************************************************/
    //region Fields and Initialization


    val cloudBroker: BrokerBG = BrokerBG(BrokerId("Cloud"), Location(39.022878, -77.464276))

    init {
        // add cloud broker to broker directory
        val tmp = brokerDirectory.brokers.toMutableMap()
        tmp[cloudBroker.brokerId] = cloudBroker
        brokerDirectory.brokers = tmp
        val tmp2 = brokerDirectory.brokerLocations.toMutableMap()
        tmp2[cloudBroker.brokerId] = cloudBroker.location
        brokerDirectory.brokerLocations = tmp2
        // wire cloud broker and assign role
        cloudBroker.assignRole(Role.Cloud)
        cloudBroker.wireBrokerDirectory(brokerDirectory)
    }

    // leader -> members
    private val broadcastGroups: Map<BrokerId, List<BrokerId>> = groupFormation()

    init {
        // assign roles to brokers
        for (leader in broadcastGroups.keys) {
            val broker = brokerDirectory.getBroker(leader) as BrokerBG
            broker.assignRole(Role.Leader)
        }
        for (member in broadcastGroups.values.flatten()) {
            val broker = brokerDirectory.getBroker(member) as BrokerBG
            broker.assignRole(Role.Member)
        }
    }

    //endregion
    /*****************************************************************/

    /*****************************************************************/
    //region Operations

    fun isLeader(brokerId: BrokerId): Boolean {
        return broadcastGroups.containsKey(brokerId)
    }

    fun isMember(brokerId: BrokerId): Boolean {
        return broadcastGroups.values.flatten().contains(brokerId)
    }

    /**
     * Returns the leader of the broker with the given [memberId].
     */
    fun getLeader(memberId: BrokerId): BrokerId {
        val group = broadcastGroups.filter { it.value.contains(memberId) }
        check(group.size == 1) { "A member has to be in exactly 1 broadcast group, is in $group" }

        return group.keys.first()
    }

    /**
     * If [brokerId] is a leader, returns all of its members.
     * If [brokerId] is a member, returns all other members.
     */
    fun getOtherGroupMembers(brokerId: BrokerId): List<BrokerId> {
        if (isLeader(brokerId)) {
            return broadcastGroups.getValue(brokerId)
        }
        if (isMember(brokerId)) {
            return broadcastGroups.getValue(getLeader(brokerId)).filter { it != brokerId }
        }
        throw IllegalStateException("$brokerId is neither a member nor a leader")
    }

    fun messageIsFromCloud(message: Message.BMessage): Boolean {
        return message.brokers[0].id == cloudBroker.brokerId
    }

    fun messageIsFromLeader(message: Message.BMessage): Boolean {
        return broadcastGroups.keys.contains(message.brokers[0].id)
    }

    fun messageIsFromOwnLeader(memberId: BrokerId, message: Message.BMessage): Boolean {
        return message.brokers[0].id == getLeader(memberId)
    }

    fun messageIsFromOwnMember(leaderId: BrokerId, message: Message.BMessage): Boolean {
        val senderId = message.brokers[0].id
        return broadcastGroups[leaderId]?.contains(senderId) ?: error("Broadcast group has no leader $leaderId")
    }

    fun messageIsFromGroup(brokerId: BrokerId, message: Message.BMessage): Boolean {
        val senderId = message.brokers[0].id
        if (isMember(brokerId)) {
            val myLeader = getLeader(brokerId)
            // the sender is my leader or his group contains the sender
            return senderId == myLeader || broadcastGroups.getValue(myLeader).contains(senderId)
        }
        // my group must contain the sender
        return broadcastGroups.getValue(brokerId).contains(senderId)

    }

    //endregion
    /*****************************************************************/

    /******************************************************************/
    //region Group formation

    /**
     * Does a broadcast group formation. Also wires the roles of each broker in the broker directory.
     */
    private fun groupFormation(): Map<BrokerId, List<BrokerId>> {
        val leaders = mutableSetOf<BrokerId>()
        while (true) {
            val nextLeader = getNextLeader(leaders) ?: break // break when all leaders assigned
            leaders.add(nextLeader)
        }

        val groups = assignMembers(leaders)
        check(leaders.size == groups.size) { "Number of leaders does not match number of broadcast groups" }
        logger.info("Assigned broadcast groups, there are ${groups.size}.")
        logger.debug("$groups")
        return groups
    }

    private fun getNextLeader(currentLeaders: Set<BrokerId>): BrokerId? {
        val candidates = clientNumbers.toList()
            .sortedByDescending { (_, clients) -> clients }.toMap() // sort descending by clients
            .filter { !currentLeaders.contains(it.key) } // only if not already part of current leaders
            .filter {
                val candidate = it.key
                // latency to all current leaders must be above threshold
                currentLeaders.all { leader ->
                    brokerDirectory.getLatencyBetweenBrokers(candidate, leader) > latencyThreshold
                }
            }
        return if (candidates.isNotEmpty()) {
            logger.trace("Next leader is ${candidates.keys.first()}")
            candidates.keys.first()
        } else {
            logger.trace("All leaders assigned")
            null
        }
    }

    private fun assignMembers(leaders: Set<BrokerId>): Map<BrokerId, List<BrokerId>> {
        val members = clientNumbers.keys.filter { !leaders.contains(it) }
        val groups = mutableMapOf<BrokerId, MutableList<BrokerId>>()
        for (member in members) {
            // assign each member to a leader
            val leader = leaders
                .map { leader -> leader to brokerDirectory.getLatencyBetweenBrokers(member, leader) }
                .minBy { (_, latency) -> latency }!!.first
            groups.getOrPut(leader) { mutableListOf() }.add(member)
        }
        // add leaders without a member
        for (leader in leaders) {
            if (!groups.containsKey(leader)) {
                groups[leader] = mutableListOf()
                logger.debug("Leader $leader does not have a member.")
            }
        }

        return groups
    }

    //endregion
    /******************************************************************/

}