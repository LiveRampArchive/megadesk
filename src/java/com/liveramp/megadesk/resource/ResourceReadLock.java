package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.data.DataCheck;

import java.util.List;

public interface ResourceReadLock<T> {

  public void acquire(String owner, DataCheck<T> dataCheck, boolean persistent) throws Exception;

  public void release(String owner) throws Exception;

  List<String> getOwners() throws Exception;
}
