package com.liveramp.megadesk.status;

import com.liveramp.megadesk.serialization.Serialization;

public class StringStatusSerialization implements Serialization<String> {

  @Override
  public byte[] serialize(String status) {
    return status.getBytes();
  }

  @Override
  public String deserialize(byte[] serializedStatus) {
    if (serializedStatus == null) {
      return null;
    } else {
      return new String(serializedStatus);
    }
  }
}
