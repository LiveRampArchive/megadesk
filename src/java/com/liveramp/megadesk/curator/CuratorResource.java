package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResource implements Resource {

  private static final String RESOURCES_PATH = "/resources";

  private final String id;
  private final String path;
  private final CuratorResourceLock lock;

  public CuratorResource(CuratorFramework curator, String id) throws Exception {
    this.id = id;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock(curator, this);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ResourceReadLock getReadLock() {
    return lock.getReadLock();
  }

  @Override
  public ResourceWriteLock getWriteLock() {
    return lock.getWriteLock();
  }

  public String getPath() {
    return path;
  }
}
