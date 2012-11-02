package com.liveramp.megadesk.device;

public interface DeviceWriteLock {

  public void acquire(String owner, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  String getOwner() throws Exception;
}
