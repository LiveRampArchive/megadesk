package com.liveramp.megadesk.driver;

import com.liveramp.megadesk.step.StepLock;

public interface StepDriver {

  public StepLock getLock();

  public byte[] getData() throws Exception;

  public void setData(byte[] data) throws Exception;
}
