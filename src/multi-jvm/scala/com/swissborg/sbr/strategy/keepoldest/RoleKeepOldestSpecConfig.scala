package com.swissborg.sbr.strategy.keepoldest

import com.swissborg.sbr.FiveNodeSpecConfig
import com.typesafe.config.ConfigFactory

object RoleKeepOldestSpecConfig extends FiveNodeSpecConfig("keepoldest/role_keep_oldest_spec.conf") {
  nodeConfig(node2, node3, node4, node5)(ConfigFactory.parseString("""akka.cluster.roles = ["foo"]"""))
}