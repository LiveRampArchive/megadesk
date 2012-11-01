package com.liveramp.megadesk.resource;

public interface ResourceReadLock {

  public void acquire(String owner, String state, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;
}
