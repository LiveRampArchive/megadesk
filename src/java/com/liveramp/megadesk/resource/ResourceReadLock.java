package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.state.StateCheck;

import java.util.List;

public interface ResourceReadLock {

  public void acquire(String owner, StateCheck stateCheck, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;
}
