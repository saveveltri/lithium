package akka.cluster.swissborg.serialization

import akka.actor.{ActorSystem, Address, ExtendedActorSystem}
import akka.cluster.Reachability.Record
import akka.cluster._
import akka.cluster.swissborg.LithiumReachability
import akka.testkit.TestKit
import com.swissborg.lithium.{LithiumReachabilityChanged, LithiumSeenChanged}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable

class LithiumClusterSerializerSpec
    extends TestKit(ActorSystem("lithium"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val serializer = new LithiumClusterSerializer(system.asInstanceOf[ExtendedActorSystem])

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

  "LithiumSeenChanged Messages" must {

    "be serializable" in {

      checkSerialization(LithiumSeenChanged(convergence = true, Set(address, address2)))
      checkSerialization(LithiumSeenChanged(convergence = false, Set(address, address2)))
      checkSerialization(LithiumSeenChanged(convergence = true, Set(address)))
      checkSerialization(LithiumSeenChanged(convergence = true, Set.empty))
    }
  }

  "LithiumReachabilityChanged Messages" must {
    "be serializable for lithium reachability built from addresses" in {
      val lithiumReachability =
        LithiumReachability(Set(uniqueAddress), Map(uniqueAddress -> Set(uniqueAddress, uniqueAddress2)))
      checkSerialization(LithiumReachabilityChanged(lithiumReachability))
    }

    "be serializable for lithium reachability built from akka reachability" in {

      val records = immutable.IndexedSeq(
        Record(uniqueAddress, uniqueAddress2, Reachability.Reachable, 1),
        Record(uniqueAddress, uniqueAddress2, Reachability.Unreachable, 2),
        Record(uniqueAddress2, uniqueAddress, Reachability.Terminated, 3)
      )
      val reachability = Reachability(records, Map(uniqueAddress -> 1, uniqueAddress2 -> 2))

      val lithiumReachability = LithiumReachability.fromReachability(reachability)
      checkSerialization(LithiumReachabilityChanged(lithiumReachability))
    }
  }
}
