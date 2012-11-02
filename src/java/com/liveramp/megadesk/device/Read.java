package com.liveramp.megadesk.device;

import com.liveramp.megadesk.data.DataCheck;

public class Read<T> {

  private final Device<T> device;
  private final DataCheck<T> dataCheck;

  public Read(Device<T> device, DataCheck<T> dataCheck) {
    this.device = device;
    this.dataCheck = dataCheck;
  }

  public Device<T> getDevice() {
    return device;
  }

  public DataCheck<T> getDataCheck() {
    return dataCheck;
  }
}
