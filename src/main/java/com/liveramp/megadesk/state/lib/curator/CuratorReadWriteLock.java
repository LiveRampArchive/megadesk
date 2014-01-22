package com.liveramp.megadesk.state.lib.curator;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class CuratorReadWriteLock implements ReadWriteLock {

  private final InterProcessReadWriteLock readWriteLock;

  public CuratorReadWriteLock(InterProcessReadWriteLock readWriteLock) {
    this.readWriteLock = readWriteLock;
  }

  @Override
  public Lock readLock() {
    return new CuratorLock(readWriteLock.readLock());
  }

  @Override
  public Lock writeLock() {
    return new CuratorLock(readWriteLock.writeLock());
  }
}
