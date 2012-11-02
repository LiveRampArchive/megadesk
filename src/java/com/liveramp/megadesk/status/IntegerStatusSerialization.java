package com.liveramp.megadesk.status;

public class IntegerStatusSerialization implements StatusSerialization<Integer> {

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
