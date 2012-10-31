package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.step.Step;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResourceWriteLock implements ResourceWriteLock {

  public CuratorResourceWriteLock(CuratorFramework curator, String path) {
  }

  @Override
  public void acquire(Step step) {
    //TODO: Implement
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public void release(Step step) {
    //TODO: Implement
    throw new RuntimeException("Not yet implemented.");
  }
}
