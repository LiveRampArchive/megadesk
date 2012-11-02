package com.liveramp.megadesk.device;

import com.liveramp.megadesk.status.check.StatusCheck;

import java.util.List;

public interface DeviceReadLock<T> {

  public void acquire(String owner, StatusCheck<T> statusCheck, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;
}
