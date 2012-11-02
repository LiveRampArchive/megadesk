package com.liveramp.megadesk.resource;

public interface Resource<T> {

  public String getId();

  public T getData() throws Exception;

  public void setData(T data) throws Exception;

  public void setData(String owner, T data) throws Exception;

  public ResourceReadLock getReadLock() throws Exception;

  public ResourceWriteLock getWriteLock() throws Exception;
}
