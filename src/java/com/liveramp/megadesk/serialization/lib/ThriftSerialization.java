package com.liveramp.megadesk.serialization.lib;

import com.liveramp.megadesk.serialization.Serialization;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

public class ThriftSerialization<T extends TBase> implements Serialization<T> {

  private final TSerializer serializer;
  private final TDeserializer deserializer;
  private final T baseObject;

  public ThriftSerialization(TProtocolFactory protocolFactory, T baseObject) {
    this.serializer = new TSerializer(protocolFactory);
    this.deserializer = new TDeserializer(protocolFactory);
    this.baseObject = baseObject;
  }

  @Override
  public byte[] serialize(T object) throws IOException {
    if (object == null) {
      return null;
    } else {
      try {
        return serializer.serialize(object);
      } catch (TException e) {
        throw new IOException("Failed to serialize Thrift object: " + object, e);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T deserialize(byte[] serializedObject) throws IOException {
    if (serializedObject == null) {
      return null;
    } else {
      T result = (T) baseObject.deepCopy();
      try {
        deserializer.deserialize(result, serializedObject);
      } catch (TException e) {
        throw new IOException("Failed to deserialize into Thrift object.", e);
      }
      return result;
    }
  }
}
