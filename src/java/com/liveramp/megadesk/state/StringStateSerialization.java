package com.liveramp.megadesk.state;

public class StringStateSerialization implements StateSerialization<String> {

  @Override
  public byte[] serialize(String state) {
    return state.getBytes();
  }

  @Override
  public String deserialize(byte[] serializedState) {
    if (serializedState == null) {
      return null;
    } else {
      return new String(serializedState);
    }
  }
}
