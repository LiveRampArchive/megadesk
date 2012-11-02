package com.liveramp.megadesk.driver;

import com.liveramp.megadesk.resource.ResourceLock;

public interface ResourceDriver {

  public ResourceLock getLock() throws Exception;

  public byte[] getData() throws Exception;

  public void setData(byte[] data) throws Exception;
}
