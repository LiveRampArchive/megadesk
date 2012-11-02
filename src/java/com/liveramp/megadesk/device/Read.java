package com.liveramp.megadesk.device;

import com.liveramp.megadesk.status.check.StatusCheck;

public class Read<T> {

  private final Device<T> device;
  private final StatusCheck statusCheck;

  public Read(Device<T> device, StatusCheck statusCheck) {
    this.device = device;
    this.statusCheck = statusCheck;
  }

  public Device<T> getDevice() {
    return device;
  }

  public StatusCheck getStatusCheck() {
    return statusCheck;
  }
}
