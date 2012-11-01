package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.step.Step;

public interface ResourceReadLock {

  public void acquire(Step step, String state) throws Exception;

  public void release(Step step, String state) throws Exception;
}
