package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;
import com.liveramp.megadesk.state.check.EqualityStateCheck;
import com.liveramp.megadesk.state.StringStateSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class StringDevice extends CuratorDevice<String> implements Device<String> {

  public StringDevice(CuratorFramework curator,
                      String id) throws Exception {
    super(curator, id, new StringStateSerialization());
  }

  public Read at(String state) {
    return new Read(this, new EqualityStateCheck<String>(state));
  }
}
