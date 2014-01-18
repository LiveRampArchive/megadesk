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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Sets;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;

public class BaseTransactionExecution implements TransactionExecution {

  public enum State {
    STANDBY,
    RUNNING,
    COMMITTED,
    ABORTED
  }

  private final BaseTransactionDependency dependency;
  private final BaseTransactionData data;
  private State state = State.STANDBY;
  private final Set<Lock> locked;

  public BaseTransactionExecution(BaseTransactionDependency dependency, BaseTransactionData data) {
    this.dependency = dependency;
    this.data = data;
    locked = Sets.newHashSet();
  }

  @Override
  public void begin() {
    ensureState(State.STANDBY);
    lock(dependency);
    state = State.RUNNING;
  }

  @Override
  public boolean tryBegin() {
    ensureState(State.STANDBY);
    boolean result = tryLock(dependency);
    if (result) {
      state = State.RUNNING;
    }
    return result;
  }

  @Override
  public void commit() {
    ensureState(State.RUNNING);
    // Write updates
    for (Map.Entry<Reference, Value> entry : data.updates().entrySet()) {
      Value value = entry.getValue();
      Reference reference = entry.getKey();
      dependency.writeDriver(reference).persistence().write(value);
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
