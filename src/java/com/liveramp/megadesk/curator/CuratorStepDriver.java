package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.driver.StepDriver;
import com.liveramp.megadesk.step.StepLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class CuratorStepDriver implements StepDriver {

  private static final String STEPS_PATH = "/megadesk/steps";

  private final CuratorFramework curator;
  private final String path;
  private final CuratorStepLock lock;

  public CuratorStepDriver(CuratorFramework curator, String id) throws Exception {
    this.curator = curator;
    this.path = ZkPath.append(STEPS_PATH, id);
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorStepLock(curator, path);
  }

  @Override
  public StepLock getLock() {
    return lock;
  }

  @Override
  public byte[] getData() throws Exception {
    return curator.getData().forPath(path);
  }

  @Override
  public void setData(byte[] data) throws Exception {
    curator.setData().forPath(path, data);
  }
}
