package com.liveramp.megadesk.resource;

public interface ResourceLock {

  public ResourceReadLock getReadLock();

  public ResourceWriteLock getWriteLock();
}
