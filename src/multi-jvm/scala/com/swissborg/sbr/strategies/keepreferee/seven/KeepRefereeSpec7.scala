package com.swissborg.sbr.strategies.keepreferee.seven

import akka.remote.transport.ThrottlerTransportAdapter.Direction
import com.swissborg.sbr.TenNodeSpec
import com.swissborg.sbr.strategies.keepreferee.KeepRefereeSpecTenNodeConfig

import scala.concurrent.duration._

class KeepRefereeSpec7MultiJvmNode1  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode2  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode3  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode4  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode5  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode6  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode7  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode8  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode9  extends KeepRefereeSpec7
class KeepRefereeSpec7MultiJvmNode10 extends KeepRefereeSpec7

/**
 * Node9 and node10 are indirectly connected in a ten node cluster
 */
class KeepRefereeSpec7 extends TenNodeSpec("KeepReferee", KeepRefereeSpecTenNodeConfig) {
  override def assertions(): Unit =
    "handle indirectly connected nodes" in within(120 seconds) {
      runOn(node1) {
        // Node9 cannot receive node10 messages
        testConductor.blackhole(node9, node10, Direction.Receive).await
      }

      enterBarrier("links-failed")

      runOn(node1, node2, node3, node4, node5, node6, node7, node8) {
        waitForSurvivors(node1, node2, node3, node4, node5, node6, node7, node8)
        waitExistsAllDownOrGone(Seq(Seq(node9), Seq(node10)))
      }

      enterBarrier("split-brain-resolved")
    }
}
