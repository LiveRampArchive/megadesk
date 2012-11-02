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
  public T getStatus() throws Exception {
    return doGetStatus();
  }

  @Override
  public void setStatus(T status) throws Exception {
    String owner = "_MANUAL_SET_STATUS_" + UUID.randomUUID().toString();
    getWriteLock().acquire(owner, false);
    try {
      doSetStatus(status);
    } finally {
      getWriteLock().release(owner);
    }
  }

  @Override
  public void setStatus(String owner, T status) throws Exception {
    getWriteLock().acquire(owner, false);
    doSetStatus(status);
    // Note: do not release the write lock as this assumes the lock is held and will be released later
  }

  protected abstract void doSetStatus(T status) throws Exception;

  protected abstract T doGetStatus() throws Exception;
}
