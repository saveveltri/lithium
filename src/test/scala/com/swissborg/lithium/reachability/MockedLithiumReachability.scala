package com.swissborg.lithium.reachability

import akka.cluster.UniqueAddress
import akka.cluster.swissborg.LithiumReachability

final case class MockedLithiumReachability(reachableNodes: Set[UniqueAddress],
                                           observersGroupedByUnreachable0: Map[UniqueAddress, Set[UniqueAddress]])
    extends LithiumReachability {

  override def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] =
    observersGroupedByUnreachable0

  override def allUnreachable: Set[UniqueAddress] = observersGroupedByUnreachable.keySet

  override def allObservers: Set[UniqueAddress] = observersGroupedByUnreachable.values.flatten.toSet

  override def isReachable(node: UniqueAddress): Boolean = reachableNodes.contains(node)

  override def remove(nodes: Set[UniqueAddress]): LithiumReachability =
    MockedLithiumReachability(
      reachableNodes.diff(nodes),
      observersGroupedByUnreachable.flatMap {
        case (unreachable, observers) =>
          if (nodes.contains(unreachable)) None
          else {
            val updateObservers = observers -- nodes
            if (updateObservers.isEmpty) None
            else Some((unreachable, updateObservers))
          }
      }
    )
}

object MockedLithiumReachability {

  def apply(reachableNodes: Set[UniqueAddress],
            observersGroupedByUnreachable0: Map[UniqueAddress, Set[UniqueAddress]]): LithiumReachability =
    MockedLithiumReachability(reachableNodes, observersGroupedByUnreachable0)

}
