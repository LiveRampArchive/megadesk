package com.liveramp.megadesk.state.lib.filesystem_tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaSerialization implements SerializationHandler {

  @Override
  public byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(o);
    oos.close();
    return bytes.toByteArray();
  }

  @Override
  public Object deserialize(byte[] bytes) throws IOException {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(byteStream);
    Object o = null;
    try {
      o = ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ois.close();
    return o;
  }
}
