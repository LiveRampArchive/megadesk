package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.step.Step;

public interface Resource {

  public String getId();

  public String getState() throws Exception;

  public void setState(Step step, String state) throws Exception;

  public ResourceReadLock getReadLock();

  public ResourceWriteLock getWriteLock();
}
