package akka.cluster.swissborg.serialization

import akka.actor.ExtendedActorSystem
import akka.cluster.Reachability.Record
import akka.cluster.swissborg.proto.lithium.LithiumReachabilityChangedProto.LithiumReachabilityProto
import akka.cluster.swissborg.proto.lithium._
import akka.cluster.swissborg.{LithiumReachability, LithiumReachabilityFromAkka, LithiumReachabilityFromNodes}
import akka.cluster.{Reachability, UniqueAddress}
import akka.serialization.{Serialization, SerializationExtension}
import com.google.protobuf.ByteString
import com.swissborg.lithium.serialization.LithiumSerializer
import com.swissborg.lithium.{LithiumReachabilityChanged, LithiumSeenChanged}

import scala.reflect.ManifestFactory

@SuppressWarnings(
  Array("org.wartremover.warts.Throw", "org.wartremover.warts.TryPartial", "org.wartremover.warts.AsInstanceOf")
)
final class LithiumClusterSerializer(val extendedSystem: ExtendedActorSystem) extends LithiumSerializer {

  override def identifier: Int = 120000

  override protected lazy val serializationExtension: Serialization = SerializationExtension(extendedSystem)

  private lazy val LithumSeenChangedManifest = ManifestFactory.classType(classOf[LithiumSeenChanged]).toString()
  private lazy val LithiumReachabilityChangedManifest =
    ManifestFactory.classType(classOf[LithiumReachabilityChanged]).toString()

  override def manifest(o: AnyRef): String = o match {
    case _: LithiumSeenChanged         => LithumSeenChangedManifest
    case _: LithiumReachabilityChanged => LithiumReachabilityChangedManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  private def serializeReachabilityStatus(status: Reachability.ReachabilityStatus): ReachabilityStatusProto =
    status match {
      case Reachability.Reachable   => ReachabilityStatusProto.Reachable
      case Reachability.Unreachable => ReachabilityStatusProto.Unreachable
      case Reachability.Terminated  => ReachabilityStatusProto.Terminated
      case value                    => throw new IllegalArgumentException(s"Can't serialize object $value in [${getClass.getName}]")
    }

  private def deserializeReachabilityStatus(status: ReachabilityStatusProto): Reachability.ReachabilityStatus =
    status match {
      case ReachabilityStatusProto.Reachable   => Reachability.Reachable
      case ReachabilityStatusProto.Unreachable => Reachability.Unreachable
      case ReachabilityStatusProto.Terminated  => Reachability.Terminated
      case value                               => throw new IllegalArgumentException(s"Can't serialize object $value in [${getClass.getName}]")
    }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case obj: LithiumSeenChanged =>
      LithiumSeenChangedProto(
        obj.convergence,
        obj.seenBy.map(address => ByteString.copyFrom(serializeAddress(address))).toSeq
      ).toByteArray
    case LithiumReachabilityChanged(LithiumReachabilityFromAkka(reachability)) =>
      val records: Seq[RecordProto] = reachability.records.map(
        record =>
          RecordProto(Some(serializeUniqueAddress(record.observer)),
                      Some(serializeUniqueAddress(record.subject)),
                      serializeReachabilityStatus(record.status),
                      record.version)
      )
      val versions = reachability.versions.map {
        case (uniqueAddress: UniqueAddress, version: Long) =>
          UniqueAddressVersionEnvelope(Some(serializeUniqueAddress(uniqueAddress)), version)
      }.toSeq
      val reachabilityProto = AkkaReachabilityProto(records, versions)
      LithiumReachabilityChangedProto(
        LithiumReachabilityProto.LithiumReachabilityFromAkka(
          LithiumReachabilityFromAkkaProto(Some(reachabilityProto))
        )
      ).toByteArray
    case LithiumReachabilityChanged(
        LithiumReachabilityFromNodes(reachableNodes, observersGroupedByUnreachable)
        ) =>
      LithiumReachabilityChangedProto(
        LithiumReachabilityProto.LithiumReachabilityFromNodes(
          LithiumReachabilityFromNodesProto(
            reachableNodes
              .map(uniqueAddress => serializeUniqueAddress(uniqueAddress))
              .toSeq,
            observersGroupedByUnreachable.map {
              case (uniqueAddress, groupUnreacheable) =>
                LithiumAddressEnvelope(
                  Some(serializeUniqueAddress(uniqueAddress)),
                  groupUnreacheable
                    .map(uniqueAddress => serializeUniqueAddress(uniqueAddress))
                    .toSeq
                )
            }.toSeq
          )
        )
      ).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case LithumSeenChangedManifest =>
        val proto = LithiumSeenChangedProto.parseFrom(bytes)
        LithiumSeenChanged(proto.convergence,
                           proto.seenBy.map(binaryAddress => deserializeAddress(binaryAddress.toByteArray)).toSet)
      case LithiumReachabilityChangedManifest =>
        val proto = LithiumReachabilityChangedProto.parseFrom(bytes)
        val lithiumReachability = proto.lithiumReachabilityProto match {
          case LithiumReachabilityProto.LithiumReachabilityFromAkka(
              LithiumReachabilityFromAkkaProto(Some(reachabilityProto))
              ) =>
            val records = reachabilityProto.records.map {
              case RecordProto(Some(observerProto), Some(subjectProto), statusProto, versionProto) =>
                Record(
                  deserializeUniqueAddress(observerProto),
                  deserializeUniqueAddress(subjectProto),
                  deserializeReachabilityStatus(statusProto),
                  versionProto
                )
              case obj => throw new IllegalArgumentException(s"Can't serialize object $obj in [${getClass.getName}]")
            }.toIndexedSeq
            val versions = reachabilityProto.versions.map {
              case UniqueAddressVersionEnvelope(Some(addressProto), version) =>
                deserializeUniqueAddress(addressProto) -> version
              case obj => throw new IllegalArgumentException(s"Can't serialize object $obj in [${getClass.getName}]")
            }.toMap
            LithiumReachability.fromReachability(Reachability(records, versions))
          case LithiumReachabilityProto.LithiumReachabilityFromNodes(
              LithiumReachabilityFromNodesProto(reachableNodes, observersGroupedByUnreachable)
              ) =>
            LithiumReachability(
              reachableNodes
                .map(deserializeUniqueAddress)
                .toSet,
              observersGroupedByUnreachable.map {
                case LithiumAddressEnvelope(Some(addressKey), addressValues) =>
                  (UniqueAddress(deserializeAddress(addressKey.address.toByteArray), addressKey.uid)
                    ->
                      addressValues
                        .map(deserializeUniqueAddress)
                        .toSet)
                case obj => throw new IllegalArgumentException(s"Can't serialize object $obj in [${getClass.getName}]")
              }.toMap
            )
          case obj => throw new IllegalArgumentException(s"Can't serialize object $obj in [${getClass.getName}]")
        }
        LithiumReachabilityChanged(lithiumReachability)
      case manifest =>
        throw new IllegalArgumentException(s"Can't serialize object of type $manifest in [${getClass.getName}]")
    }
}
