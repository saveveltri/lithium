package com.swissborg.lithium

package utils

import cats._
import com.swissborg.lithium.strategy._

import scala.collection.SortedSet

/**
 * Represents the cluster after applying a strategy's decision.
 */
final case class PostResolution(
  /**
   * The nodes as seen by each partition.
   */
  partitions: List[SortedSet[Node]]
) {

  /**
   * True if there are no non-reachable nodes in the cluster and
   * that all the partitions have the same elements, aka the
   * resolution did not create multiple clusters.
   */
  lazy val isResolved: Boolean = {
    val nonEmptyPartitions = partitions.filter(_.nonEmpty)

    val allReachableNodes = nonEmptyPartitions.forall(_.forall {
      case _: ReachableNode => true
      case _                => false
    })

    lazy val noSplitBrain = if (nonEmptyPartitions.lengthCompare(1) > 0) {
      nonEmptyPartitions.tail
        .foldLeft((true, nonEmptyPartitions.head)) {
          case ((noSplitBrain, expectedPartition), partition) =>
            (noSplitBrain && expectedPartition.iterator.sameElements(partition.iterator), expectedPartition)
        }
        ._1
    } else {
      // 1 or less non-empty partitions
      true
    }

    allReachableNodes && noSplitBrain
  }
}

object PostResolution {
  val empty: PostResolution = PostResolution(List.empty)

  def one(nodes: SortedSet[Node]): PostResolution = PostResolution(List(nodes))

  def fromDecision(worldView: WorldView)(decision: Decision): PostResolution =
    decision match {
      // In all these cases the entire partition will down itself.
      case _: Decision.DownReachable                        => PostResolution.empty
      case Decision.DownThese(_: Decision.DownReachable, _) => PostResolution.empty
      case Decision.DownThese(_, _: Decision.DownReachable) => PostResolution.empty

      case _ =>
        val nodesAfterDowning = worldView.nodes.toSortedSet -- decision.nodesToDown
        PostResolution.one(nodesAfterDowning)
    }

  implicit val postResolutionMonoid: Monoid[PostResolution] = new Monoid[PostResolution] {
    override def empty: PostResolution = PostResolution.empty

    override def combine(x: PostResolution, y: PostResolution): PostResolution =
      PostResolution(x.partitions ++ y.partitions)
  }
}
