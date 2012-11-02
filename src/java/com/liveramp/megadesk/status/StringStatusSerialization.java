package com.liveramp.megadesk.status;

public class StringStatusSerialization implements StatusSerialization<String> {

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
