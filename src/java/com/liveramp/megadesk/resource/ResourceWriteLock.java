package com.liveramp.megadesk.resource;

public interface ResourceWriteLock {

  public void acquire(String owner, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  String getOwner() throws Exception;

  boolean isOwnedByAnother(String id) throws Exception;
}
