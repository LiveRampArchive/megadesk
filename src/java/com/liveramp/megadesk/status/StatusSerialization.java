package com.liveramp.megadesk.status;

public interface StatusSerialization<T> {

  public byte[] serialize(T status);

  public T deserialize(byte[] serializedStatus);
}
