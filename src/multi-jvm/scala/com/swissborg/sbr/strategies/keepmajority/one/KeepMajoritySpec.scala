package com.swissborg.sbr.strategies.keepmajority.one

import akka.remote.transport.ThrottlerTransportAdapter.Direction
import com.swissborg.sbr.TestUtil.linksToKillForPartitions
import com.swissborg.sbr.ThreeNodeSpec
import com.swissborg.sbr.strategies.keepmajority.KeepMajoritySpecThreeNodeConfig

import scala.concurrent.duration._

class KeepMajoritySpecMultiJvmNode1 extends KeepMajoritySpec
class KeepMajoritySpecMultiJvmNode2 extends KeepMajoritySpec
class KeepMajoritySpecMultiJvmNode3 extends KeepMajoritySpec

class KeepMajoritySpec extends ThreeNodeSpec("KeepMajority", KeepMajoritySpecThreeNodeConfig) {
  override def assertions(): Unit =
    "Bidirectional link failure" in within(60 seconds) {
      runOn(node1) {
        // Partition with node1 and node2 <- survive
        // Partition with node3           <- killed
        linksToKillForPartitions(List(node1, node2) :: List(node3) :: Nil).foreach {
          case (from, to) => testConductor.blackhole(from, to, Direction.Both).await
        }
      }

      enterBarrier("links-failed")

      runOn(node1, node2) {
        waitToBecomeUnreachable(node3)
      }

      enterBarrier("split-brain")

      runOn(node1, node2) {
        waitForSurvivors(node1, node2)
        waitForDownOrGone(node3)
      }

      runOn(node3) {
        waitForSelfDowning
      }

      enterBarrier("split-brain-resolved")
    }
}
