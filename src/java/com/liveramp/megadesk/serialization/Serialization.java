package com.liveramp.megadesk.serialization;

public interface Serialization<T> {

  public byte[] serialize(T value);

  public T deserialize(byte[] serializedValue);
}
