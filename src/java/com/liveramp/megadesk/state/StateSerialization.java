package com.liveramp.megadesk.state;

public interface StateSerialization<T> {

  public byte[] serialize(T state);

  public T deserialize(byte[] serializedState);
}
