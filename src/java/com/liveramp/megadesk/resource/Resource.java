package com.liveramp.megadesk.resource;

public interface Resource {

  ResourceReadLock getReadLock();

  ResourceWriteLock getWriteLock();
}
