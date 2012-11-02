package com.liveramp.megadesk.device;

public interface Device<T> {

  public String getId();

  public T getData() throws Exception;

  public void setData(T data) throws Exception;

  public void setData(String owner, T data) throws Exception;

  public DeviceReadLock<T> getReadLock();

  public DeviceWriteLock getWriteLock();
}
