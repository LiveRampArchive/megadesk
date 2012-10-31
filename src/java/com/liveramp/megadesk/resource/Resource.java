package com.liveramp.megadesk.resource;

public interface Resource {

  public String getId();

  public ResourceReadLock getReadLock();

  public ResourceWriteLock getWriteLock();
}
