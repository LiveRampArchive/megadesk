package com.liveramp.megadesk.resource;

import java.util.List;

public interface ResourceReadLock {

  public void acquire(String owner, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;

  boolean isOwnedByAnother(String id) throws Exception;
}
