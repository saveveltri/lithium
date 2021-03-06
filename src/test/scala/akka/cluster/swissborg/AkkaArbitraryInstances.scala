package akka.cluster.swissborg

import akka.actor.{ActorPath, Address, ChildActorPath, RootActorPath}
import akka.cluster.{Member, UniqueAddress, Reachability => _}
import com.swissborg.lithium.instances.ArbitraryTestInstances._
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import shapeless.tag
import shapeless.tag.@@

/**
 * Arbitrary instances that can only be generated from within the `akka.cluster` namespace.
 */
object AkkaArbitraryInstances {
  sealed trait JoiningTag
  type JoiningMember = Member @@ JoiningTag

  implicit val arbJoiningMember: Arbitrary[JoiningMember] = Arbitrary {
    for {
      uniqueAddress <- arbitrary[UniqueAddress]
    } yield tag[JoiningTag][Member](Member(uniqueAddress, Set("dc-datacenter")))
  }

  implicit val arbRootActorPath: Arbitrary[RootActorPath] = Arbitrary(arbitrary[Address].map(RootActorPath(_)))

  def arbChildActorPath(parent: ActorPath): Arbitrary[ChildActorPath] =
    Arbitrary(for {
      c   <- Gen.alphaChar
      cs  <- Gen.alphaStr
      uid <- Gen.chooseNum(0, Int.MaxValue)
      name = s"$c$cs"
    } yield new ChildActorPath(parent, name, uid))

  def arbActorPath(depth: Int, parent: ActorPath): Arbitrary[ActorPath] =
    Arbitrary(
      if (depth <= 0) Gen.const(parent)
      else arbChildActorPath(parent).arbitrary.flatMap(arbActorPath(depth - 1, _).arbitrary)
    )

  implicit val arbActorPath0: Arbitrary[ActorPath] = Arbitrary(for {
    depth  <- Gen.chooseNum(0, 10)
    parent <- arbitrary[RootActorPath]
    path   <- arbActorPath(depth, parent).arbitrary
  } yield path)
}
