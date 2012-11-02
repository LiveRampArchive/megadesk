package com.liveramp.megadesk.status;

import com.liveramp.megadesk.serialization.Serialization;

public class IntegerStatusSerialization implements Serialization<Integer> {

  @Override
  public byte[] serialize(Integer status) {
    if (status == null) {
      return null;
    } else {
      return status.toString().getBytes();
    }
  }

  @Override
  public Integer deserialize(byte[] serializedStatus) {
    if (serializedStatus == null) {
      return null;
    } else {
      return Integer.valueOf(new String(serializedStatus));
    }
  }
}
