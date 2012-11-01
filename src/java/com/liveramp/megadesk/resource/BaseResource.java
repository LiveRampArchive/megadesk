package com.liveramp.megadesk.resource;

import java.util.UUID;

public abstract class BaseResource<T> implements Resource<T> {

  private final String id;

  protected BaseResource(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public T getState() throws Exception {
    return doGetState();
  }

  @Override
  public void setState(T state) throws Exception {
    String owner = "_MANUAL_SET_STATE_" + UUID.randomUUID().toString();
    getWriteLock().acquire(owner, false);
    try {
      doSetState(state);
    } finally {
      getWriteLock().release(owner);
    }
  }

  @Override
  public void setState(String owner, T state) throws Exception {
    getWriteLock().acquire(owner, false);
    doSetState(state);
    // Note: do not release the write lock as this assumes the lock is held and will be released later
  }

  protected abstract void doSetState(T state) throws Exception;

  protected abstract T doGetState() throws Exception;
}
