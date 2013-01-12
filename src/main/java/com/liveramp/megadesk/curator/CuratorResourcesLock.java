package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourcesLock;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorResourcesLock implements ResourcesLock {

  private final InterProcessMutex lock;

  public CuratorResourcesLock(CuratorFramework curator, String path) {
    this.lock = new InterProcessMutex(curator, path);
  }

  @Override
  public void acquire() throws Exception {
    lock.acquire();
  }

  @Override
  public void release() throws Exception {
    lock.release();
  }

  @Override
  public boolean isAcquiredInThisProcess() {
    return lock.isAcquiredInThisProcess();
  }
}
