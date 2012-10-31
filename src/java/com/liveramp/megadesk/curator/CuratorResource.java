package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResource implements Resource {

  private final CuratorResourceLock lock;

  public CuratorResource(CuratorFramework curator, String path) {
    this.lock = new CuratorResourceLock(curator, path);
  }

  @Override
  public ResourceReadLock getReadLock() {
    return lock.getReadLock();
  }

  @Override
  public ResourceWriteLock getWriteLock() {
    return lock.getWriteLock();
  }
}
