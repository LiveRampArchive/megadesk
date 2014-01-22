package com.liveramp.megadesk.state.lib.curator;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class CuratorLock implements Lock {

  private final InterProcessLock lock;

  public CuratorLock(InterProcessLock lock) {
    this.lock = lock;
  }

  @Override
  public void lock() {
    try {
      lock.acquire();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    try {
      lock.acquire();
    } catch (InterruptedException interrupted) {
      throw interrupted;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean tryLock() {
    try {
      return lock.acquire(1, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
    try {
      return lock.acquire(l, timeUnit);
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unlock() {
    try {
      lock.release();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Condition newCondition() {
    throw new NotImplementedException();
  }
}
