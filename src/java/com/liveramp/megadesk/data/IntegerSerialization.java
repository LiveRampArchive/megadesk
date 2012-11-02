package com.liveramp.megadesk.data;

import com.liveramp.megadesk.serialization.Serialization;

public class IntegerSerialization implements Serialization<Integer> {

  @Override
  public byte[] serialize(Integer data) {
    if (data == null) {
      return null;
    } else {
      return data.toString().getBytes();
    }
  }

  @Override
  public Integer deserialize(byte[] serializedData) {
    if (serializedData == null) {
      return null;
    } else {
      return Integer.valueOf(new String(serializedData));
    }
  }
}
