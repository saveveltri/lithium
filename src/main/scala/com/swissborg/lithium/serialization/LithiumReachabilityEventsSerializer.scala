package com.swissborg.lithium.serialization

import akka.actor.ExtendedActorSystem
import akka.cluster.swissborg.proto.lithium._
import akka.serialization.{Serialization, SerializationExtension}
import com.swissborg.lithium.reporter.SplitBrainReporter.{NodeIndirectlyConnected, NodeReachable, NodeUnreachable}

import scala.reflect.ManifestFactory

@SuppressWarnings(
  Array("org.wartremover.warts.Throw", "org.wartremover.warts.TryPartial", "org.wartremover.warts.AsInstanceOf")
)
final class LithiumReachabilityEventsSerializer(val extendedSystem: ExtendedActorSystem) extends LithiumSerializer {

  override def identifier: Int = 130000

  override protected lazy val serializationExtension: Serialization = SerializationExtension(extendedSystem)

  private lazy val NodeReachableManifest = ManifestFactory.classType(classOf[NodeReachable]).toString()
  private lazy val NodeIndirectlyConnectedManifest =
    ManifestFactory.classType(classOf[NodeIndirectlyConnected]).toString()
  private lazy val NodeUnreachableManifest = ManifestFactory.classType(classOf[NodeUnreachable]).toString()

  override def manifest(o: AnyRef): String = o match {
    case _: NodeReachable           => NodeReachableManifest
    case _: NodeIndirectlyConnected => NodeIndirectlyConnectedManifest
    case _: NodeUnreachable         => NodeUnreachableManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case NodeReachable(node)           => NodeReachableProto(Some(serializeUniqueAddress(node))).toByteArray
    case NodeIndirectlyConnected(node) => NodeIndirectlyConnectedProto(Some(serializeUniqueAddress(node))).toByteArray
    case NodeUnreachable(node)         => NodeUnreachableProto(Some(serializeUniqueAddress(node))).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case NodeReachableManifest =>
        val proto = NodeReachableProto.parseFrom(bytes)
        proto.node match {
          case Some(uniqueAddressProto) => NodeReachable(deserializeUniqueAddress(uniqueAddressProto))
          case None                     => throw new IllegalArgumentException(s"Can't deserialize object $proto in [${getClass.getName}]")
        }
      case NodeIndirectlyConnectedManifest =>
        val proto = NodeIndirectlyConnectedProto.parseFrom(bytes)
        proto.node match {
          case Some(uniqueAddressProto) => NodeIndirectlyConnected(deserializeUniqueAddress(uniqueAddressProto))
          case None                     => throw new IllegalArgumentException(s"Can't deserialize object $proto in [${getClass.getName}]")
        }
      case NodeUnreachableManifest =>
        val proto = NodeUnreachableProto.parseFrom(bytes)
        proto.node match {
          case Some(uniqueAddressProto) => NodeUnreachable(deserializeUniqueAddress(uniqueAddressProto))
          case None                     => throw new IllegalArgumentException(s"Can't deserialize object $proto in [${getClass.getName}]")
        }
      case manifest =>
        throw new IllegalArgumentException(s"Can't serialize object of type $manifest in [${getClass.getName}]")
    }
}
