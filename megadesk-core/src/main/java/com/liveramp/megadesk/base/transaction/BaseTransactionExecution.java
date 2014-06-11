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

package com.liveramp.megadesk.base.transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.liveramp.megadesk.core.state.Lock;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.DependencyType;
import com.liveramp.megadesk.core.transaction.TransactionExecution;
import com.liveramp.megadesk.core.transaction.VariableDependency;

public class BaseTransactionExecution implements TransactionExecution {

  public enum State {
    STANDBY,
    RUNNING,
    COMMITTED,
    ABORTED
  }

  private Dependency dependency;
  private Context data;
  private State state = State.STANDBY;
  private final Set<Lock> locksAcquired;

  public BaseTransactionExecution() {
    locksAcquired = Sets.newHashSet();
  }

  @Override
  public Context begin(Dependency dependency) {
    ensureState(State.STANDBY);
    lock(dependency);
    return prepare(dependency);
  }

  @Override
  public Context tryBegin(Dependency dependency) {
    ensureState(State.STANDBY);
    boolean result = tryLock(dependency);
    if (result) {
      return prepare(dependency);
    } else {
      return null;
    }
  }

  private Context prepare(Dependency dependency) {
    this.data = new BaseContext(dependency);
    this.state = State.RUNNING;
    this.dependency = dependency;
    return this.data;
  }

  @Override
  public void commit() {
    ensureState(State.RUNNING);
    // Writes
    for (Variable variable : dependency.writes()) {
      Object value = data.read(variable);
      variable.driver().persistence().write(value);
    }
    // Release execution locks
    unlock(locksAcquired);
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    ensureState(State.RUNNING);
    unlock(locksAcquired);
    state = State.ABORTED;
  }

  private boolean tryLock(Dependency dependency) {
    return tryLockAndRemember(orderedLocks(dependency), locksAcquired);
  }

  private void lock(Dependency dependency) {
    lockAndRemember(orderedLocks(dependency), locksAcquired);
  }

  // Locks are globally ordered to prevent deadlocks
  private static List<Lock> orderedLocks(Dependency dependency) {
    List<VariableDependency> all = Lists.newArrayList(dependency.all());
    Collections.sort(all);
    List<Lock> result = Lists.newArrayList();
    for (VariableDependency variableDependency : all) {
      result.add(variableDependency.lock());
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
      throw new IllegalStateException("State should be " + state + " but is " + this.state);
    }
  }
}
