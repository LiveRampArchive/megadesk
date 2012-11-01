package com.liveramp.megadesk.resource;

public interface Resource {

  public String getId();

  public String getState() throws Exception;

  public void setState(String state) throws Exception;

  public void setState(String owner, String state) throws Exception;

  public ResourceReadLock getReadLock();

  public ResourceWriteLock getWriteLock();
}
