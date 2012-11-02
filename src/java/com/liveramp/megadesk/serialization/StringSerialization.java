package com.liveramp.megadesk.serialization;

public class StringSerialization implements Serialization<String> {

  @Override
  public byte[] serialize(String data) {
    if (data == null) {
      return null;
    } else {
      return data.getBytes();
    }
  }

  @Override
  public String deserialize(byte[] serializedData) {
    if (serializedData == null) {
      return null;
    } else {
      return new String(serializedData);
    }
  }
}
