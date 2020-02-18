package akka.cluster.swissborg.serialization

import akka.actor.ExtendedActorSystem
import akka.cluster.MemberStatus._
import akka.cluster.swissborg.proto.common.{MemberProto, MemberStatusProto}
import akka.cluster.swissborg.proto.lithiumSplitBrain.NodeProtoWrapper.NodeProto
import akka.cluster.swissborg.proto.lithiumSplitBrain._
import akka.cluster.{Member, MemberStatus}
import akka.serialization.{Serialization, SerializationExtension}
import com.swissborg.lithium._
import com.swissborg.lithium.resolver.SplitBrainResolver.{DownAll, ResolveSplitBrain}
import com.swissborg.lithium.serialization.LithiumSerializer

import scala.collection.immutable.SortedSet
import scala.reflect.ManifestFactory

@SuppressWarnings(
  Array("org.wartremover.warts.Throw", "org.wartremover.warts.OptionPartial", "org.wartremover.warts.AsInstanceOf")
)
final class LithiumSplitBrainEventsSerializer(val extendedSystem: ExtendedActorSystem) extends LithiumSerializer {

  override def identifier: Int = 130000

  override protected lazy val serializationExtension: Serialization = SerializationExtension(extendedSystem)

  private lazy val ResolveSplitBrainManifest = ManifestFactory.classType(classOf[ResolveSplitBrain]).toString()
  private lazy val DownAllManifest           = ManifestFactory.classType(classOf[DownAll]).toString()

  override def manifest(o: AnyRef): String = o match {
    case _: ResolveSplitBrain => ResolveSplitBrainManifest
    case _: DownAll           => DownAllManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  private def serializeMemberStatus(status: MemberStatus): MemberStatusProto = status match {
    case Joining  => MemberStatusProto.Joining
    case WeaklyUp => MemberStatusProto.WeaklyUp
    case Up       => MemberStatusProto.Up
    case Leaving  => MemberStatusProto.Leaving
    case Exiting  => MemberStatusProto.Exiting
    case Down     => MemberStatusProto.Down
    case Removed  => MemberStatusProto.Removed
  }

  private def deserializeMemberStatus(status: MemberStatusProto): MemberStatus = status match {
    case MemberStatusProto.Joining  => Joining
    case MemberStatusProto.WeaklyUp => WeaklyUp
    case MemberStatusProto.Up       => Up
    case MemberStatusProto.Leaving  => Leaving
    case MemberStatusProto.Exiting  => Exiting
    case MemberStatusProto.Down     => Down
    case MemberStatusProto.Removed  => Removed
  }

  private def serializeMember(member: Member) =
    MemberProto(Some(serializeUniqueAddress(member.uniqueAddress)),
                member.upNumber,
                serializeMemberStatus(member.status),
                member.roles.toSeq)

  private def deserializeMember(memberProto: MemberProto): Member =
    new Member(deserializeUniqueAddress(memberProto.address.get),
               memberProto.upNumber,
               deserializeMemberStatus(memberProto.status),
               memberProto.roles.toSet)

  private def serializeNode(node: Node): NodeProtoWrapper = node match {
    case ReachableNode(member) =>
      NodeProtoWrapper(NodeProto.ReachableNodeProto(ReachableNodeProto(Some(serializeMember(member)))))
    case UnreachableNode(member) =>
      NodeProtoWrapper(NodeProto.UnreachableNodeProto(UnreachableNodeProto(Some(serializeMember(member)))))
    case IndirectlyConnectedNode(member) =>
      NodeProtoWrapper(NodeProto.IndirectlyConnectedNode(IndirectlyConnectedNodeProto(Some(serializeMember(member)))))
  }

  private def deserializeNode(node: NodeProtoWrapper): Node = node.nodeProto match {
    case NodeProto.ReachableNodeProto(ReachableNodeProto(Some(member))) => ReachableNode(deserializeMember(member))
    case NodeProto.UnreachableNodeProto(UnreachableNodeProto(Some(member))) =>
      UnreachableNode(deserializeMember(member))
    case NodeProto.IndirectlyConnectedNode(IndirectlyConnectedNodeProto(Some(member))) =>
      IndirectlyConnectedNode(deserializeMember(member))
  }

  private def serializeWorldView(worldView: WorldView): WorldViewProto =
    WorldViewProto(Some(serializeNode(worldView.selfNode)),
                   (worldView.nodes - worldView.selfNode).toSet.map(serializeNode).toSeq)

  private def deserializeWorldView(worldViewProto: WorldViewProto) =
    WorldView.fromNodes(deserializeNode(worldViewProto.selfNode.get),
                        SortedSet(worldViewProto.otherNodes.map(deserializeNode): _*))

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case ResolveSplitBrain(worldView) => ResolveSplitBrainProto(Some(serializeWorldView(worldView))).toByteArray
    case DownAll(worldView)           => DownAllProto(Some(serializeWorldView(worldView))).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case ResolveSplitBrainManifest =>
        val proto = ResolveSplitBrainProto.parseFrom(bytes)
        ResolveSplitBrain(deserializeWorldView(proto.worldView.get))
      case DownAllManifest =>
        val proto = DownAllProto.parseFrom(bytes)
        DownAll(deserializeWorldView(proto.worldView.get))
      case manifest =>
        throw new IllegalArgumentException(s"Can't serialize object of type $manifest in [${getClass.getName}]")
    }
}
