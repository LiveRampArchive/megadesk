package com.liveramp.megadesk.state.check;

import com.liveramp.megadesk.device.Device;

public interface StateCheck<T> {

  public boolean check(Device<T> device) throws Exception;
}
