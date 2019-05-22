package com.swissborg.sbr.strategies.keepmajority.two

import akka.remote.transport.ThrottlerTransportAdapter.Direction
import com.swissborg.sbr.strategies.keepmajority.KeepMajoritySpecFiveNodeConfig
import com.swissborg.sbr.{FiveNodeSpec, TestUtil}

import scala.concurrent.duration._

class KeepMajoritySpec2MultiJvmNode1 extends KeepMajoritySpec2
class KeepMajoritySpec2MultiJvmNode2 extends KeepMajoritySpec2
class KeepMajoritySpec2MultiJvmNode3 extends KeepMajoritySpec2
class KeepMajoritySpec2MultiJvmNode4 extends KeepMajoritySpec2
class KeepMajoritySpec2MultiJvmNode5 extends KeepMajoritySpec2

class KeepMajoritySpec2 extends FiveNodeSpec("KeepMajority", KeepMajoritySpecFiveNodeConfig) {
  override def assertions(): Unit =
    "Three partitions, bidirectional link failure" in within(60 seconds) {
      runOn(node1) {
        // Partition with node1, node2, node3 <- survive
        // Partition with node 4              <- killed
        // Partition with node 5              <- killed
        TestUtil
          .linksToKillForPartitions(List(node1, node2, node3) :: List(node4) :: List(node5) :: Nil)
          .foreach {
            case (from, to) => testConductor.blackhole(from, to, Direction.Both).await
          }
      }

      enterBarrier("links-failed")

      runOn(node1, node2, node3) {
        waitToBecomeUnreachable(node4, node5)
      }

      runOn(node4) {
        waitToBecomeUnreachable(node1, node2, node3, node5)
      }

      runOn(node5) {
        waitToBecomeUnreachable(node1, node2, node3, node4)
      }

      enterBarrier("split-brain")

      runOn(node1, node2, node3) {
        waitForSurvivors(node1, node2, node3)
        waitForDownOrGone(node4, node5)
      }

      runOn(node4, node5) {
        waitForSelfDowning
      }

      enterBarrier("split-brain-resolved")
    }
}
