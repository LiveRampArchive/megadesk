package com.liveramp.megadesk.device;

import java.util.UUID;

public abstract class BaseDevice<T> implements Device<T> {

  private final String id;

  protected BaseDevice(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public T getData() throws Exception {
    return doGetData();
  }

  @Override
  public void setData(T data) throws Exception {
    String owner = "_MANUAL_SET_STATUS_" + UUID.randomUUID().toString();
    getWriteLock().acquire(owner, false);
    try {
      doSetData(data);
    } finally {
      getWriteLock().release(owner);
    }
  }

  @Override
  public void setData(String owner, T data) throws Exception {
    getWriteLock().acquire(owner, false);
    doSetData(data);
    // Note: do not release the write lock as this assumes the lock is held and will be released later
  }

  protected abstract T doGetData() throws Exception;

  protected abstract void doSetData(T data) throws Exception;
}
