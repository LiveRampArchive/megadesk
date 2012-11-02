package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;
import com.liveramp.megadesk.status.IntegerStatusSerialization;
import com.liveramp.megadesk.status.check.ComparisonStatusCheck;
import com.netflix.curator.framework.CuratorFramework;

public class IntegerDevice extends CuratorDevice<Integer> implements Device<Integer> {

  public IntegerDevice(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new IntegerStatusSerialization());
  }

  public Read at(Integer version) {
    return new Read(this, new ComparisonStatusCheck<Integer>(version, 0));
  }

  public Read lessThan(final Integer version) {
    return new Read(this, new ComparisonStatusCheck<Integer>(version, -1));
  }

  public Read greaterThan(final Integer version) {
    return new Read(this, new ComparisonStatusCheck<Integer>(version, 1));
  }
}
