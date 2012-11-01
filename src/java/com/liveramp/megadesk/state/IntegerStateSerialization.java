package com.liveramp.megadesk.state;

public class IntegerStateSerialization implements StateSerialization<Integer> {

  @Override
  public byte[] serialize(Integer state) {
    if (state == null) {
      return null;
    } else {
      return state.toString().getBytes();
    }
  }

  @Override
  public Integer deserialize(byte[] serializedState) {
    if (serializedState == null) {
      return null;
    } else {
      return Integer.valueOf(new String(serializedState));
    }
  }
}
