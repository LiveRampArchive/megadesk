package com.liveramp.megadesk.data;

import com.liveramp.megadesk.device.Device;

public class EqualityDataCheck<T> implements DataCheck<T> {

  private final T data;

  public EqualityDataCheck(T data) {
    this.data = data;
  }

  @Override
  public boolean check(Device<T> device) throws Exception {
    T currentData = device.getData();
    return (data == null && currentData == null)
        || (data != null && data.equals(currentData));
  }

  @Override
  public String toString() {
    return "[EqualityDataCheck: '" + data + "']";
  }
}
