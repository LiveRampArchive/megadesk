package com.liveramp.megadesk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import com.liveramp.megadesk.resource.ResourcesLock;

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
