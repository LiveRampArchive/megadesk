package com.liveramp.megadesk.step;

public interface StepLock {

  public void acquire() throws Exception;

  public void release() throws Exception;
}
