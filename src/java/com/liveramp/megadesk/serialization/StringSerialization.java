package com.liveramp.megadesk.serialization;

import com.liveramp.megadesk.serialization.Serialization;

public class StringSerialization implements Serialization<String> {

  @Override
  public byte[] serialize(String data) {
    return data.getBytes();
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
