package com.liveramp.megadesk.device;

import com.liveramp.megadesk.data.check.DataCheck;

public class Read<T> {

  private final Device<T> device;
  private final DataCheck dataCheck;

  public Read(Device<T> device, DataCheck dataCheck) {
    this.device = device;
    this.dataCheck = dataCheck;
  }

  public Device<T> getDevice() {
    return device;
  }

  public DataCheck getDataCheck() {
    return dataCheck;
  }
}
