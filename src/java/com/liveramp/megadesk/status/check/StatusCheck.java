package com.liveramp.megadesk.status.check;

import com.liveramp.megadesk.device.Device;

public interface StatusCheck<T> {

  public boolean check(Device<T> device) throws Exception;
}
