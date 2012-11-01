package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.step.Step;

public interface ResourceWriteLock {

  public void acquire(Step step) throws Exception;

  public void release(Step step) throws Exception;
}
