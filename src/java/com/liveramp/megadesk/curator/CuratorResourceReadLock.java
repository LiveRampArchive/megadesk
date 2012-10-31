package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.step.Step;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResourceReadLock implements ResourceReadLock {

  public CuratorResourceReadLock(CuratorFramework curator, String path) {

  }

  @Override
  public void acquire(Step step) throws Exception {
    //TODO: Implement
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public void release(Step step) throws Exception {
    //TODO: Implement
    throw new RuntimeException("Not yet implemented.");
  }
}
