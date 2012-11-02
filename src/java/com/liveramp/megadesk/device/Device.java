package com.liveramp.megadesk.device;

public interface Device<T> {

  public String getId();

  public T getStatus() throws Exception;

  public void setStatus(T status) throws Exception;

  public void setStatus(String owner, T status) throws Exception;

  public DeviceReadLock getReadLock();

  public DeviceWriteLock getWriteLock();
}
