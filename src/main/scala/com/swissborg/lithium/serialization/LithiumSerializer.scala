package com.swissborg.lithium.serialization

import akka.actor.{Address, ExtendedActorSystem}
import akka.cluster.swissborg.proto.lithium.LithiumSeenChangedProto
import akka.serialization.{Serialization, SerializationExtension, SerializerWithStringManifest, Serializers}
import com.google.protobuf.ByteString
import com.swissborg.lithium.internals.{LithiumReachabilityChanged, LithiumSeenChanged}

import scala.reflect.ManifestFactory

@SuppressWarnings(
  Array("org.wartremover.warts.Throw", "org.wartremover.warts.TryPartial", "org.wartremover.warts.AsInstanceOf")
)
final class LithiumSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override def identifier: Int = 120

  private val serializationExtension: Serialization = SerializationExtension(system)

  private val LithumSeenChangedManifest = ManifestFactory.classType(classOf[LithiumSeenChanged]).toString()
  private val lithiumReachabilityChangedManifest =
    ManifestFactory.classType(classOf[LithiumReachabilityChanged]).toString()

  private val addressSerializer = serializationExtension.serializerFor(classOf[Address])
  private val dummyAddress      = Address("", "")

  override def manifest(o: AnyRef): String = o match {
    case _: LithiumSeenChanged         => LithumSeenChangedManifest
    case _: LithiumReachabilityChanged => lithiumReachabilityChangedManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case obj: LithiumSeenChanged =>
      LithiumSeenChangedProto(
        obj.convergence,
        obj.seenBy.map(address => ByteString.copyFrom(addressSerializer.toBinary(address))).toSeq
      ).toByteArray
    case _: LithiumReachabilityChanged => throw new IllegalStateException("not yet implemented")
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case LithumSeenChangedManifest =>
        val proto = LithiumSeenChangedProto.parseFrom(bytes)
        LithiumSeenChanged(
          proto.convergence,
          proto.seenBy
            .map(
              binaryAddress =>
                serializationExtension
                  .deserialize(binaryAddress.toByteArray,
                               addressSerializer.identifier,
                               Serializers.manifestFor(addressSerializer, dummyAddress))
                  .get
                  .asInstanceOf[Address]
            )
            .toSet
        )
      case lithiumReachabilityChangedManifest => throw new IllegalStateException("not yet implemented")
      case manifest =>
        throw new IllegalArgumentException(s"Can't serialize object of type $manifest in [${getClass.getName}]")
    }
}
