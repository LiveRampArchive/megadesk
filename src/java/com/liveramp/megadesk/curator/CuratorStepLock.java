package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.step.StepLock;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorStepLock implements StepLock {

  private final InterProcessMutex lock;

  public CuratorStepLock(CuratorFramework curator, String path) {
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
}
