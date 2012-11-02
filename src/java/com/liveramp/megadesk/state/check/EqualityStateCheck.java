package com.liveramp.megadesk.state.check;

import com.liveramp.megadesk.device.Device;

public class EqualityStateCheck<T> implements StateCheck<T> {

  private final T state;

  public EqualityStateCheck(T state) {
    this.state = state;
  }

  @Override
  public boolean check(Device<T> device) throws Exception {
    T currentState = device.getState();
    return (state == null && currentState == null)
        || (state != null && state.equals(currentState));
  }

  @Override
  public String toString() {
    return "[EqualityStateCheck: '" + state + "']";
  }
}
