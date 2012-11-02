package com.liveramp.megadesk.data.check;

import com.liveramp.megadesk.device.Device;

public interface DataCheck<T> {

  public boolean check(Device<T> device) throws Exception;
}
