package akka.cluster.swissborg.serialization

import akka.actor.{ActorSystem, Address, ExtendedActorSystem}
import akka.cluster._
import akka.testkit.TestKit
import com.swissborg.lithium.resolver.SplitBrainResolver.{DownAll, ResolveSplitBrain}
import com.swissborg.lithium.{IndirectlyConnectedNode, ReachableNode, UnreachableNode, WorldView}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable.SortedSet

class LithiumSplitBrainEventsSerializerSpec
    extends TestKit(ActorSystem("lithium"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val serializer = new LithiumSplitBrainEventsSerializer(system.asInstanceOf[ExtendedActorSystem])

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

  "ResolveSplitBrain Messages" must {

    "be serializable" in {

      checkSerialization(
        ResolveSplitBrain(
          WorldView.fromNodes(
            ReachableNode(Member(uniqueAddress, Set("dc-role2"))),
            SortedSet(
              ReachableNode(Member(uniqueAddress, Set("dc-role1", "dc-role2"))),
              UnreachableNode(Member(uniqueAddress, Set("dc-role1", "dc-role2"))),
              IndirectlyConnectedNode(Member(uniqueAddress, Set("dc-role1", "dc-role2")))
            )
          )
        )
      )

    }
  }

  "DownAll Messages" must {

    "be serializable" in {

      checkSerialization(
        DownAll(
          WorldView.fromNodes(
            ReachableNode(Member(uniqueAddress, Set("dc-role2"))),
            SortedSet(
              ReachableNode(Member(uniqueAddress, Set("dc-role1", "dc-role2"))),
              UnreachableNode(Member(uniqueAddress, Set("dc-role1", "dc-role2"))),
              IndirectlyConnectedNode(Member(uniqueAddress, Set("dc-role1", "dc-role2")))
            )
          )
        )
      )
    }
  }

}
