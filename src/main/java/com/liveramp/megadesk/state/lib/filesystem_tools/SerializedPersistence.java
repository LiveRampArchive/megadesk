package com.liveramp.megadesk.state.lib.filesystem_tools;

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.lib.BasePersistence;

import java.io.IOException;

public abstract class SerializedPersistence<VALUE> extends BasePersistence<VALUE> implements Persistence<VALUE> {
  private final SerializationHandler<VALUE> serializer;

  protected SerializedPersistence(SerializationHandler<VALUE> serializer) {
    this.serializer = serializer;
  }

  @Override
  public VALUE read() {
    byte[] data = readBytes();
    VALUE value;
    try {
      value = serializer.deserialize(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return value;
  }

  @Override
  public void write(VALUE value) {
    try {
      writeBytes(serializer.serialize(value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void writeBytes(byte[] serializedObject);

  protected abstract byte[] readBytes();
}
