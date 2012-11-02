package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.serialization.Serialization;

import java.util.UUID;

public abstract class BaseResource<T> implements Resource<T> {

  private final String id;
  private final ResourceDriver driver;
  private final Serialization<T> dataSerialization;

  protected BaseResource(String id,
                         ResourceDriver driver,
                         Serialization<T> dataSerialization) {
    this.id = id;
    this.driver = driver;
    this.dataSerialization = dataSerialization;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public T getData() throws Exception {
    return dataSerialization.deserialize(driver.getData());
  }

  private void doSetData(T data) throws Exception {
    driver.setData(dataSerialization.serialize(data));
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

  @Override
  public ResourceReadLock getReadLock() throws Exception {
    return driver.getLock().getReadLock();
  }

  @Override
  public ResourceWriteLock getWriteLock() throws Exception {
    return driver.getLock().getWriteLock();
  }

  @Override
  public String toString() {
    try {
      return "[BaseResource '" + getId()
          + "', writer: '" + getWriteLock().getOwner()
          + "', readers: " + getReadLock().getOwners() + "]";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
