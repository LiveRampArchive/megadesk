package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.ManeuverLock;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorManeuverLock implements ManeuverLock {

  private final InterProcessMutex lock;

  public CuratorManeuverLock(CuratorFramework curator, String path) {
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
