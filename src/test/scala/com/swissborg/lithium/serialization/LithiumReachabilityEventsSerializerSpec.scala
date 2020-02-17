package com.swissborg.lithium.serialization

import akka.actor.{ActorSystem, Address, ExtendedActorSystem}
import akka.cluster._
import akka.testkit.TestKit
import com.swissborg.lithium.reporter.SplitBrainReporter.{NodeIndirectlyConnected, NodeReachable, NodeUnreachable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class LithiumReachabilityEventsSerializerSpec
    extends TestKit(ActorSystem("lithium"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val serializer = new LithiumReachabilityEventsSerializer(system.asInstanceOf[ExtendedActorSystem])

  def roundtrip[T <: AnyRef](obj: T): T = {
    val manifest = serializer.manifest(obj)
    val blob     = serializer.toBinary(obj)
    serializer.fromBinary(blob, manifest).asInstanceOf[T]
  }

  def checkSerialization(obj: AnyRef): Unit =
    obj should ===(roundtrip(obj))

  private val address        = Address("akka", "system", "some.host.org", 4711)
  private val uniqueAddress  = UniqueAddress(address, 17L)
  private val address2       = Address("akka", "system", "other.host.org", 4711)
  private val uniqueAddress2 = UniqueAddress(address2, 18L)

  "Lithium NodeReachabilityEvent Messages" must {

    "be serializable" in {
      checkSerialization(NodeReachable(uniqueAddress))
      checkSerialization(NodeReachable(uniqueAddress2))

      checkSerialization(NodeIndirectlyConnected(uniqueAddress))
      checkSerialization(NodeIndirectlyConnected(uniqueAddress2))

      checkSerialization(NodeUnreachable(uniqueAddress))
      checkSerialization(NodeUnreachable(uniqueAddress2))
    }
  }
}
