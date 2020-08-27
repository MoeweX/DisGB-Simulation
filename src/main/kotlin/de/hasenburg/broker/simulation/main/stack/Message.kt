package de.hasenburg.broker.simulation.main.stack

import de.hasenburg.broker.simulation.main.broker.BrokerId
import de.hasenburg.broker.simulation.main.client.ClientId
import de.hasenburg.broker.simulation.main.misc.Event
import de.hasenburg.broker.simulation.main.misc.Subscription
import de.hasenburg.broker.simulation.main.misc.Tick
import de.hasenburg.broker.simulation.main.misc.Topic
import de.hasenburg.broker.simulation.main.stack.Message.BMessage.*
import de.hasenburg.broker.simulation.main.stack.Message.CMessage.*
import de.hasenburg.geobroker.commons.model.spatial.Location

sealed class Message : Comparable<Message> {

    abstract val origin: CTarget

    /**
     * The broker that will be processing the message.
     */
    abstract val us: BrokerId

    /**
     * The tick in which [us] processes the message.
     */
    abstract val now: Tick

    override fun compareTo(other: Message): Int {
        return origin.compareTo(other.origin)
    }

    /*****************************************************************
     * Client Messages (Messages between a client and a broker)
     ****************************************************************/

    sealed class CMessage : Message() {

        abstract val processor: BTarget

        override val us: BrokerId
            get() = processor.id

        override val now: Tick
            get() = processor.tick

        data class CLocationUpdate(val newLocation: Location,
                                   override val origin: CTarget,
                                   override val processor: BTarget) : CMessage() {

            fun forward(to: BTarget): BLocationUpdate {
                return BLocationUpdate(newLocation, origin, listOf(processor, to))
            }

        }

        data class CSubscriptionUpdate(val subscription: Subscription,
                                       override val origin: CTarget,
                                       override val processor: BTarget) : CMessage() {

            fun forward(to: BTarget): BSubscriptionUpdate {
                return BSubscriptionUpdate(subscription, origin, listOf(processor, to))
            }

        }

        data class CSubscriptionRemoval(val topic: Topic,
                                        override val origin: CTarget,
                                        override val processor: BTarget) : CMessage() {

            fun forward(to: BTarget): BSubscriptionRemoval {
                return BSubscriptionRemoval(topic, origin, listOf(processor, to))
            }

        }

        data class CEventMatching(val event: Event,
                                  override val origin: CTarget,
                                  override val processor: BTarget) : CMessage() {

            fun createCEventDelivery(subscriber: CTarget): CEventDelivery {
                return CEventDelivery(subscriber, origin, processor)
            }

            fun createBEventDelivery(subscribers: Set<ClientId>, target: BTarget): Message {
                return BEventDelivery(subscribers, origin, listOf(processor, target))
            }

            fun forward(to: BTarget): BEventMatching {
                return BEventMatching(event, origin, listOf(processor, to))
            }

        }

        /**
         * In difference to all other messages, this message is received by a client rather than a broker.
         * As this simulation uses only brokers for processing, [us] links to the sending broker (i.e., this broker
         * virtually handles the processing of client messages), while [now] links to the Tick the client receives this
         * message.
         */
        data class CEventDelivery(val subscriber: CTarget,
                                  override val origin: CTarget,
                                  override val processor: BTarget) : CMessage() {

            override val now: Tick
                get() = subscriber.tick

        }

    }

    /*****************************************************************
     * Broker Messages (messages between two brokers)
     ****************************************************************/

    sealed class BMessage : Message() {

        /**
         * Should always contain two brokers; the processor (sender) and the next target (receiver)
         */
        abstract val brokers: List<BTarget>

        override val us: BrokerId
            get() = brokers[1].id

        override val now: Tick
            get() = brokers[1].tick

        data class BLocationUpdate(val newLocation: Location,
                                   override val origin: CTarget,
                                   override val brokers: List<BTarget>) : BMessage()

        data class BSubscriptionUpdate(val subscription: Subscription,
                                       override val origin: CTarget,
                                       override val brokers: List<BTarget>) : BMessage() {

            fun forward(to: BTarget): BSubscriptionUpdate {
                return BSubscriptionUpdate(subscription, origin, listOf(brokers[1], to))
            }

        }

        data class BSubscriptionRemoval(val topic: Topic,
                                        override val origin: CTarget,
                                        override val brokers: List<BTarget>) : BMessage() {

            fun forward(to: BTarget): Message {
                return BSubscriptionRemoval(topic, origin, listOf(brokers[1], to))
            }

        }

        data class BEventMatching(val event: Event,
                                  override val origin: CTarget,
                                  override val brokers: List<BTarget>) : BMessage() {

            fun createCEventDelivery(subscriber: CTarget): CEventDelivery {
                return CEventDelivery(subscriber, origin, brokers[1])
            }

            fun createBEventDelivery(subscribers: Set<ClientId>, target: BTarget): Message {
                return BEventDelivery(subscribers, origin, listOf(brokers[1], target))
            }

            fun forward(to: BTarget): Message {
                return BEventMatching(event, origin, listOf(brokers[1], to))
            }

        }

        data class BEventDelivery(val subscribers: Set<ClientId>,
                                  override val origin: CTarget,
                                  override val brokers: List<BTarget>) : BMessage() {

            fun createCEventDeliveries(arrivalTick: Tick): List<CEventDelivery> {
                return subscribers
                    .map { CTarget(it, arrivalTick) }
                    .map { CEventDelivery(it, origin, brokers[1]) }
            }

        }

    }

}