package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.data.EqualityDataCheck;
import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;
import com.liveramp.megadesk.serialization.StringSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class StringDevice extends CuratorDevice<String> implements Device<String> {

  public StringDevice(CuratorFramework curator,
                      String id) throws Exception {
    super(curator, id, new StringSerialization());
  }

  public Read at(String data) {
    return new Read<String>(this, new EqualityDataCheck<String>(data));
  }
}
