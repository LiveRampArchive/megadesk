package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;
import com.liveramp.megadesk.state.IntegerStateSerialization;
import com.liveramp.megadesk.state.check.ComparisonStateCheck;
import com.netflix.curator.framework.CuratorFramework;

public class VersionedDevice extends CuratorDevice<Integer> implements Device<Integer> {

  public VersionedDevice(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new IntegerStateSerialization());
  }

  public Read at(Integer version) {
    return new Read(this, new ComparisonStateCheck<Integer>(version, 0));
  }

  public Read lessThan(final Integer version) {
    return new Read(this, new ComparisonStateCheck<Integer>(version, -1));
  }

  public Read greaterThan(final Integer version) {
    return new Read(this, new ComparisonStateCheck<Integer>(version, 1));
  }
}
