package com.liveramp.megadesk.data;

import com.liveramp.megadesk.device.Device;

public class NotEmptyStringDataCheck implements DataCheck<String> {

  @Override
  public boolean check(Device<String> device) throws Exception {
    String data = device.getData();
    return data != null && data.length() > 0;
  }
}
