package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResource implements Resource {

  private final CuratorResourceReadLock readLock;
  private final CuratorResourceWriteLock writeLock;

  public CuratorResource(CuratorFramework curator, String path) {
    this.readLock = new CuratorResourceReadLock(curator, path);
    this.writeLock = new CuratorResourceWriteLock(curator, path);
  }

  @Override
  public ResourceReadLock getReadLock() {
    return readLock;
  }

  @Override
  public ResourceWriteLock getWriteLock() {
    return writeLock;
  }
}
