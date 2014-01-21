/**
 *  Copyright 2014 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.megadesk.transaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Value;

public class BaseTransactionExecution implements TransactionExecution {

  public enum State {
    STANDBY,
    RUNNING,
    COMMITTED,
    ABORTED
  }

  private Dependency dependency;
  private Transaction data;
  private State state = State.STANDBY;
  private final Set<Lock> executionLocksAcquired;
  private final Set<Lock> persistenceLocksAcquired;

  public BaseTransactionExecution() {
    executionLocksAcquired = Sets.newHashSet();
    persistenceLocksAcquired = Sets.newHashSet();
  }

  @Override
  public Transaction begin(Dependency dependency) {
    ensureState(State.STANDBY);
    lock(dependency);
    return prepare(dependency);
  }

  @Override
  public Transaction tryBegin(Dependency dependency) {
    ensureState(State.STANDBY);
    boolean result = tryLock(dependency);
    if (result) {
      return prepare(dependency);
    } else {
      return null;
    }
  }

  private Transaction prepare(Dependency dependency) {
    // Acquire persistence read locks for snapshot
    lockAndRemember(persistenceReadLocks(dependency), persistenceLocksAcquired);
    try {
      this.data = new BaseTransaction(dependency);
    } finally {
      // Always release persistence read locks
      unlock(persistenceLocksAcquired);
    }
    // Prepare rest of transaction
    this.state = State.RUNNING;
    this.dependency = dependency;
    return this.data;
  }

  @Override
  public void commit() {
    ensureState(State.RUNNING);
    // Acquire persistence write locks
    lockAndRemember(persistenceWriteLocks(dependency), persistenceLocksAcquired);
    try {
      // Write updates
      for (Driver driver : dependency.writes()) {
        Value value = data.binding(driver.reference()).read();
        driver.persistence().write(value);
      }
    } finally {
      // Always release persistence write locks
      unlock(persistenceLocksAcquired);
    }
    // Release execution locks
    unlock(executionLocksAcquired);
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    ensureState(State.RUNNING);
    unlock(executionLocksAcquired);
    state = State.ABORTED;
  }

  private boolean tryLock(Dependency dependency) {
    return tryLockAndRemember(executionReadLocks(dependency), executionLocksAcquired)
               && tryLockAndRemember(executionWriteLocks(dependency), executionLocksAcquired);
  }

  private void lock(Dependency dependency) {
    lockAndRemember(executionReadLocks(dependency), executionLocksAcquired);
    lockAndRemember(executionWriteLocks(dependency), executionLocksAcquired);
  }

  private static List<Lock> executionReadLocks(Dependency dependency) {
    List<Lock> result = Lists.newArrayList();
    for (Driver driver : dependency.reads()) {
      result.add(driver.executionLock().readLock());
    }
    return result;
  }

  private static List<Lock> executionWriteLocks(Dependency dependency) {
    List<Lock> result = Lists.newArrayList();
    for (Driver driver : dependency.writes()) {
      result.add(driver.executionLock().writeLock());
    }
    return result;
  }

  private static List<Lock> persistenceReadLocks(Dependency dependency) {
    List<Lock> result = Lists.newArrayList();
    // Snapshots
    for (Driver driver : dependency.snapshots()) {
      result.add(driver.persistenceLock().readLock());
    }
    // Execution reads
    for (Driver driver : dependency.reads()) {
      result.add(driver.persistenceLock().readLock());
    }
    // Execution writes
    for (Driver driver : dependency.writes()) {
      result.add(driver.persistenceLock().readLock());
    }
    return result;
  }

  private static List<Lock> persistenceWriteLocks(Dependency dependency) {
    List<Lock> result = Lists.newArrayList();
    // Execution writes only
    for (Driver driver : dependency.writes()) {
      result.add(driver.persistenceLock().writeLock());
    }
    return result;
  }

  private static boolean tryLockAndRemember(Collection<Lock> locks, Set<Lock> acquiredLocks) {
    for (Lock lock : locks) {
      if (!tryLockAndRemember(lock, acquiredLocks)) {
        unlock(acquiredLocks);
        return false;
      }
    }
    return true;
  }

  private static void lockAndRemember(Collection<Lock> locks, Set<Lock> acquiredLocks) {
    for (Lock lock : locks) {
      lockAndRemember(lock, acquiredLocks);
    }
  }

  private static boolean tryLockAndRemember(Lock lock, Set<Lock> acquiredLocks) {
    boolean result = lock.tryLock();
    if (result) {
      acquiredLocks.add(lock);
    }
    return result;
  }

  private static void lockAndRemember(Lock lock, Set<Lock> acquiredLocks) {
    lock.lock();
    acquiredLocks.add(lock);
  }

  private static void unlock(Set<Lock> acquiredLocks) {
    Iterator<Lock> lockIterator = acquiredLocks.iterator();
    while (lockIterator.hasNext()) {
      lockIterator.next().unlock();
      lockIterator.remove();
    }
  }

  private void ensureState(State state) {
    if (this.state != state) {
      throw new IllegalStateException("TransactionExecution state should be " + state + " but is " + this.state);
    }
  }
}
