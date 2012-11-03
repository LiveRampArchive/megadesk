package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.Megadesk;
import com.liveramp.megadesk.driver.MainDriver;
import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.driver.StepDriver;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorMegadesk implements Megadesk {

  private final CuratorFramework curator;
  private final CuratorMainDriver mainDriver;

  public CuratorMegadesk(CuratorFramework curator) {
    this.curator = curator;
    this.mainDriver = new CuratorMainDriver(curator);
  }

  @Override
  public MainDriver getMainDriver() {
    return mainDriver;
  }

  @Override
  public ResourceDriver getResourceDriver(String id) throws Exception {
    return new CuratorResourceDriver(curator, id);
  }

  @Override
  public StepDriver getStepDriver(String id) throws Exception {
    return new CuratorStepDriver(curator, id);
  }
}
