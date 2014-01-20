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

import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Sets;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Value;

public class BaseTransaction implements Transaction {

  public enum State {
    STANDBY,
    RUNNING,
    COMMITTED,
    ABORTED
  }

  private TransactionDependency dependency;
  private State state = State.STANDBY;
  private final Set<Lock> locked;

  public BaseTransaction() {
    locked = Sets.newHashSet();
  }

  @Override
  public TransactionData begin(TransactionDependency dependency) {
    ensureState(State.STANDBY);
    this.dependency = dependency;
    lock(dependency);
    state = State.RUNNING;
    return new BaseTransactionData(dependency);
  }

  @Override
  public TransactionData tryBegin(TransactionDependency dependency) {
    ensureState(State.STANDBY);
    this.dependency = dependency;
    boolean result = tryLock(dependency);
    if (result) {
      state = State.RUNNING;
      return new BaseTransactionData(dependency);
    } else {
      return null;
    }
  }

  @Override
  public void commit(TransactionData data) {
    ensureState(State.RUNNING);
    // Write updates
    for (Driver driver : dependency.writes()) {
      Value value = data.get(driver.reference()).read();
      driver.persistence().write(value);
    }
    // Release locks
    unlock();
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    ensureState(State.RUNNING);
    unlock();
    state = State.ABORTED;
  }

  private boolean tryLock(TransactionDependency dependency) {
    for (Driver read : dependency.reads()) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (dependency.writes().contains(read)) {
        continue;
      }
      if (!read.lock().readLock().tryLock()) {
        unlock();
        return false;
      }
    }
    for (Driver write : dependency.writes()) {
      if (!write.lock().writeLock().tryLock()) {
        unlock();
        return false;
      }
    }
    return true;
  }

  private void lock(Lock lock) {
    lock.lock();
    locked.add(lock);
  }

  private void lock(TransactionDependency dependency) {
    for (Driver read : dependency.reads()) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (dependency.writes().contains(read)) {
        continue;
      }
      lock(read.lock().readLock());
    }
    for (Driver write : dependency.writes()) {
      lock(write.lock().writeLock());
    }
  }

  private void unlock() {
    for (Lock lock : locked) {
      lock.unlock();
    }
  }

  private void ensureState(State state) {
    if (this.state != state) {
      throw new IllegalArgumentException("Transaction state should be " + state + " but is " + this.state);
    }
  }
}
