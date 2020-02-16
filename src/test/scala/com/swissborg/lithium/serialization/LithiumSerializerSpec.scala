package com.swissborg.lithium.serialization

//package akka.cluster.protobuf

import akka.actor.{ActorSystem, Address, ExtendedActorSystem}
import akka.cluster._
import akka.testkit.TestKit
import com.swissborg.lithium.internals.LithiumSeenChanged
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
//import akka.cluster.InternalClusterAction.CompatibleConfig
//import akka.testkit.{ AkkaSpec, TestKit }
//import com.github.ghik.silencer.silent
import com.typesafe.config.ConfigFactory

class LithiumSerializerSpec
    extends TestKit(ActorSystem("lithium", ConfigFactory.load("application-test.conf")))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val serializer = new LithiumSerializer(system.asInstanceOf[ExtendedActorSystem])

  def roundtrip[T <: AnyRef](obj: T): T = {
    val manifest = serializer.manifest(obj)
    val blob     = serializer.toBinary(obj)
    serializer.fromBinary(blob, manifest).asInstanceOf[T]
  }

  def checkSerialization(obj: AnyRef): Unit =
    obj should ===(roundtrip(obj))
//    (obj, roundtrip(obj)) match {
//      case (env: GossipEnvelope, env2: GossipEnvelope) =>
//        env2.from should ===(env.from)
//        env2.to should ===(env.to)
//        env2.gossip should ===(env.gossip)
//      case (_, ref) =>
//        ref should ===(obj)
//    }

  private def roundtripWithManifest[T <: AnyRef](obj: T, manifest: String): T = {
    val blob = serializer.toBinary(obj)
    serializer.fromBinary(blob, manifest).asInstanceOf[T]
  }

  private def checkDeserializationWithManifest(obj: AnyRef, deserializationManifest: String): Unit =
    obj should ===(roundtripWithManifest(obj, deserializationManifest))
//    match {
//      case (env: GossipEnvelope, env2: GossipEnvelope) =>
//        env2.from should ===(env.from)
//        env2.to should ===(env.to)
//        env2.gossip should ===(env.gossip)
//      case (_, ref) =>
//        ref should ===(obj)
//    }

//  val a1 = TestMember(Address("akka", "sys", "a", 2552), Joining, Set.empty[String])
//  val b1 = TestMember(Address("akka", "sys", "b", 2552), Up, Set("r1"))
//  val c1 = TestMember(Address("akka", "sys", "c", 2552), Leaving, Set.empty[String], "foo")
//  val d1 = TestMember(Address("akka", "sys", "d", 2552), Exiting, Set("r1"), "foo")
//  val e1 = TestMember(Address("akka", "sys", "e", 2552), Down, Set("r3"))
//  val f1 = TestMember(Address("akka", "sys", "f", 2552), Removed, Set("r3"), "foo")

  "Lithium Messages" must {

    "be serializable" in {
      val address        = Address("akka", "system", "some.host.org", 4711)
      val uniqueAddress  = UniqueAddress(address, 17L)
      val address2       = Address("akka", "system", "other.host.org", 4711)
      val uniqueAddress2 = UniqueAddress(address2, 18L)

      checkSerialization(LithiumSeenChanged(convergence = true, Set(address, address2)))


      /*val node1 = VectorClock.Node("node1")
      val node2 = VectorClock.Node("node2")
      val node3 = VectorClock.Node("node3")
      val node4 = VectorClock.Node("node4")
      val g1 = (Gossip(SortedSet(a1, b1, c1, d1)) :+ node1 :+ node2).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      val g2 = (g1 :+ node3 :+ node4).seen(a1.uniqueAddress).seen(c1.uniqueAddress)
      val reachability3 = Reachability.empty
        .unreachable(a1.uniqueAddress, e1.uniqueAddress)
        .unreachable(b1.uniqueAddress, e1.uniqueAddress)
      val g3 =
        g2.copy(members = SortedSet(a1, b1, c1, d1, e1), overview = g2.overview.copy(reachability = reachability3))
      val g4 = g1.remove(d1.uniqueAddress, 352684800)
      checkSerialization(GossipEnvelope(a1.uniqueAddress, uniqueAddress2, g1))
      checkSerialization(GossipEnvelope(a1.uniqueAddress, uniqueAddress2, g2))
      checkSerialization(GossipEnvelope(a1.uniqueAddress, uniqueAddress2, g3))
      checkSerialization(GossipEnvelope(a1.uniqueAddress, uniqueAddress2, g4))

      checkSerialization(GossipStatus(a1.uniqueAddress, g1.version))
      checkSerialization(GossipStatus(a1.uniqueAddress, g2.version))
      checkSerialization(GossipStatus(a1.uniqueAddress, g3.version))

      checkSerialization(InternalClusterAction.Welcome(uniqueAddress, g2))*/
    }

    // can be removed in 2.6.3 only checks deserialization with new not yet in effect manifests for 2.6.2

  }

//  "Rolling upgrades for heart beat message changes in 2.5.23" must {
//    // FIXME, add issue for serializing this as the new message type
//
//    "serialize heart beats as Address to support versions prior or 2.5.23" in {
//      serializer.manifest(ClusterHeartbeatSender.Heartbeat(a1.address, -1, -1)) should ===(
//        ClusterMessageSerializer.HeartBeatManifestPre2523)
//    }
//
//    "serialize heart beat responses as UniqueAddress to support versions prior to 2.5.23" in {
//      serializer.manifest(ClusterHeartbeatSender.HeartbeatRsp(a1.uniqueAddress, -1, -1)) should ===(
//        ClusterMessageSerializer.HeartBeatRspManifest2523)
//    }
//
//    "be able to deserialize HeartBeat protobuf message" in {
//      val hbProtobuf = cm.Heartbeat
//        .newBuilder()
//        .setFrom(serializer.addressToProto(a1.address))
//        .setSequenceNr(1)
//        .setCreationTime(2)
//        .build()
//        .toByteArray
//
//      serializer.fromBinary(hbProtobuf, ClusterMessageSerializer.HeartbeatManifest) should ===(
//        ClusterHeartbeatSender.Heartbeat(a1.address, 1, 2))
//    }
//
//    "be able to deserialize HeartBeatRsp probuf message" in {
//      val hbrProtobuf = cm.HeartBeatResponse
//        .newBuilder()
//        .setFrom(serializer.uniqueAddressToProto(a1.uniqueAddress))
//        .setSequenceNr(1)
//        .setCreationTime(2)
//        .build()
//        .toByteArray
//
//      serializer.fromBinary(hbrProtobuf, ClusterMessageSerializer.HeartbeatRspManifest) should ===(
//        ClusterHeartbeatSender.HeartbeatRsp(a1.uniqueAddress, 1, 2))
//    }
//  }

//  "Cluster router pool" must {
//    "be serializable with no role" in {
//      checkSerialization(
//        ClusterRouterPool(
//          RoundRobinPool(nrOfInstances = 4),
//          ClusterRouterPoolSettings(totalInstances = 2, maxInstancesPerNode = 5, allowLocalRoutees = true)))
//    }
//
//    "be serializable with one role" in {
//      checkSerialization(
//        ClusterRouterPool(
//          RoundRobinPool(nrOfInstances = 4),
//          ClusterRouterPoolSettings(
//            totalInstances = 2,
//            maxInstancesPerNode = 5,
//            allowLocalRoutees = true,
//            useRoles = Set("Richard, Duke of Gloucester"))))
//    }
//
//    "be serializable with many roles" in {
//      checkSerialization(
//        ClusterRouterPool(
//          RoundRobinPool(nrOfInstances = 4),
//          ClusterRouterPoolSettings(
//            totalInstances = 2,
//            maxInstancesPerNode = 5,
//            allowLocalRoutees = true,
//            useRoles = Set("Richard, Duke of Gloucester", "Hongzhi Emperor", "Red Rackham"))))
//    }
//  }

}
