package com.swissborg.lithium.serialization

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.swissborg.proto.lithium.UniqueAddressProto
import akka.serialization.{Serialization, SerializerWithStringManifest, Serializers}
import com.google.protobuf.ByteString

@SuppressWarnings(Array("org.wartremover.warts.TryPartial", "org.wartremover.warts.AsInstanceOf"))
trait LithiumSerializer extends SerializerWithStringManifest {

  protected def serializationExtension: Serialization

  private lazy val addressSerializer = serializationExtension.serializerFor(classOf[Address])
  private lazy val dummyAddress      = Address("", "")

  protected def serializeUniqueAddress(uniqueAddress: UniqueAddress): UniqueAddressProto =
    UniqueAddressProto(ByteString.copyFrom(addressSerializer.toBinary(uniqueAddress.address)), uniqueAddress.longUid)

  protected def deserializeUniqueAddress(uniqueAddressProto: UniqueAddressProto): UniqueAddress =
    UniqueAddress(deserializeAddress(uniqueAddressProto.address.toByteArray), uniqueAddressProto.uid)

  protected def serializeAddress(address: Address): Array[Byte] = addressSerializer.toBinary(address)

  protected def deserializeAddress(address: Array[Byte]): Address =
    serializationExtension
      .deserialize(address, addressSerializer.identifier, Serializers.manifestFor(addressSerializer, dummyAddress))
      .get
      .asInstanceOf[Address]
}
