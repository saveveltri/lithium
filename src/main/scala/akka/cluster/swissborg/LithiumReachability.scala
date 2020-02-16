package akka.cluster.swissborg

import akka.cluster._

sealed trait LithiumReachability {
  def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]]

  def allUnreachable: Set[UniqueAddress]

  def allObservers: Set[UniqueAddress]

  def isReachable(node: UniqueAddress): Boolean

  /**
   * Removes the records mentioning any of the `nodes`.
   */
  def remove(nodes: Set[UniqueAddress]): LithiumReachability
}

final case class LithiumReachabilityFromAkka(r: Reachability) extends LithiumReachability {

  override def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] =
    r.observersGroupedByUnreachable

  override def allUnreachable: Set[UniqueAddress] = r.allUnreachable

  override def allObservers: Set[UniqueAddress] = r.allObservers

  override def isReachable(node: UniqueAddress): Boolean = r.isReachable(node)

  override def remove(nodes: Set[UniqueAddress]): LithiumReachability = LithiumReachabilityFromAkka(r.remove(nodes))
}

final case class LithiumReachabilityFromNodes(reachableNodes: Set[UniqueAddress],
                                              observersGroupedByUnreachableIn: Map[UniqueAddress, Set[UniqueAddress]])
    extends LithiumReachability {

  override def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] =
    observersGroupedByUnreachableIn

  override def allUnreachable: Set[UniqueAddress] = observersGroupedByUnreachable.keySet

  override def allObservers: Set[UniqueAddress] = observersGroupedByUnreachable.values.flatten.toSet

  override def isReachable(node: UniqueAddress): Boolean = reachableNodes.contains(node)

  override def remove(nodes: Set[UniqueAddress]): LithiumReachability =
    LithiumReachabilityFromNodes(
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

object LithiumReachability {

  def fromReachability(r: Reachability): LithiumReachability = LithiumReachabilityFromAkka(r)

  // Used for testing
  def apply(reachableNodes: Set[UniqueAddress],
            observersGroupedByUnreachable0: Map[UniqueAddress, Set[UniqueAddress]]): LithiumReachability =
    LithiumReachabilityFromNodes(reachableNodes, observersGroupedByUnreachable0)
}
