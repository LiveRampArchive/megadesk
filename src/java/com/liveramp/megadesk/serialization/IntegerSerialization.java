package com.liveramp.megadesk.serialization;

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
    if (serializedData == null || serializedData.length == 0) {
      return null;
    } else {
      return Integer.valueOf(new String(serializedData));
    }
  }
}
