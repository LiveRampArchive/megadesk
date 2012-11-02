package com.liveramp.megadesk.device;

import com.liveramp.megadesk.status.check.StatusCheck;

public class Read {

  private final Device device;
  private final StatusCheck statusCheck;

  public Read(Device device, StatusCheck statusCheck) {
    this.device = device;
    this.statusCheck = statusCheck;
  }

  public Device getDevice() {
    return device;
  }

  public StatusCheck getStatusCheck() {
    return statusCheck;
  }
}
