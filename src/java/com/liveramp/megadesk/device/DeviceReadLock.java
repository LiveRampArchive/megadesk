package com.liveramp.megadesk.device;

import com.liveramp.megadesk.data.check.DataCheck;

import java.util.List;

public interface DeviceReadLock<T> {

  public void acquire(String owner, DataCheck<T> dataCheck, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;
}
