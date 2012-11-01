package com.liveramp.megadesk.resource;

public interface Resource<T> {

  public String getId();

  public T getState() throws Exception;

  public void setState(T state) throws Exception;

  public void setState(String owner, T state) throws Exception;

  public ResourceReadLock getReadLock();

  public ResourceWriteLock getWriteLock();
}
