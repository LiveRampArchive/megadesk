package com.liveramp.megadesk.resource;

public interface ResourcesLock {

  public void acquire() throws Exception;

  public void release() throws Exception;

  boolean isAcquiredInThisProcess();
}
