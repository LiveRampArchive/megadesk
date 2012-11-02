package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.resource.ResourceLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResourceDriver implements ResourceDriver {

  private static final String RESOURCES_PATH = "/megadesk/resources";

  private final CuratorFramework curator;
  private final String path;
  private final ResourceLock lock;

  public CuratorResourceDriver(CuratorFramework curator, String id) throws Exception {
    this.curator = curator;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock(curator, path);
  }

  @Override
  public ResourceLock getLock() throws Exception {
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
