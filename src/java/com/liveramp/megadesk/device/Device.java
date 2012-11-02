package com.liveramp.megadesk.device;

public interface Device<T> {

  public String getId();

  public T getState() throws Exception;

  public void setState(T state) throws Exception;

  public void setState(String owner, T state) throws Exception;

  public DeviceReadLock getReadLock();

  public DeviceWriteLock getWriteLock();
}
