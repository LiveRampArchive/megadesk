package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.base.state.BaseDriver;
import com.liveramp.megadesk.core.state.ReadWriteLock;
import com.liveramp.megadesk.core.state.Reference;

public class BaseTimestampedDriver<VALUE> extends BaseDriver<VALUE> implements TimestampedDriver<VALUE> {

  public BaseTimestampedDriver(
      Reference<VALUE> reference,
      TimestampedPersistence<VALUE> persistence,
      ReadWriteLock persistenceLock,
      ReadWriteLock executionLock) {
    super(reference, persistence, persistenceLock, executionLock);
  }

  @Override
  public TimestampedPersistence<VALUE> persistence() {
    return (TimestampedPersistence<VALUE>) super.persistence();
  }

  @Override
  public long modified() {
    this.persistenceLock().readLock().lock();
    long modifedTime = this.persistence().modified();
    this.persistenceLock().readLock().unlock();
    return modifedTime;
  }
}
