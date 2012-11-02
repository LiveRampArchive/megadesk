package com.liveramp.megadesk.device;

import com.liveramp.megadesk.state.check.StateCheck;

import java.util.List;

public interface DeviceReadLock<T> {

  public void acquire(String owner, StateCheck<T> stateCheck, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;
}
