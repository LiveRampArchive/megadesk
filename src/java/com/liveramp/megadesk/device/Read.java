package com.liveramp.megadesk.device;

import com.liveramp.megadesk.state.check.StateCheck;

public class Read {

  private final Device device;
  private final StateCheck stateCheck;

  public Read(Device device, StateCheck stateCheck) {
    this.device = device;
    this.stateCheck = stateCheck;
  }

  public Device getDevice() {
    return device;
  }

  public StateCheck getStateCheck() {
    return stateCheck;
  }
}
