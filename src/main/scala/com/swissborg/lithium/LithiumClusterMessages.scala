package com.swissborg.lithium

import akka.actor.Address
import akka.cluster.swissborg.LithiumReachability

/**
 * Marker used to configure the serialization bindings
 */
trait LithiumClusterMessage

/**
 * Mirror of [[akka.cluster.ClusterEvent.ReachabilityChanged]]
 * so it can be subscribed to by actors in other packages.
 */
final case class LithiumReachabilityChanged(reachability: LithiumReachability) extends LithiumClusterMessage

/**
 * Mirror of [[akka.cluster.ClusterEvent.SeenChanged]]
 * so it can be subscribed to by actors in other packages.
 */
final case class LithiumSeenChanged(convergence: Boolean, seenBy: Set[Address]) extends LithiumClusterMessage
