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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.state.MultiLock;
import com.liveramp.megadesk.core.state.Lock;
import com.liveramp.megadesk.core.state.MultiPersistenceTransaction;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
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
  private Context context;
  private State state = State.STANDBY;
  private Lock lock;

  @Override
  public Context begin(Dependency dependency) {
    ensureState(State.STANDBY);
    lock = dependencyLock(dependency);
    lock.lock();
    return prepare(dependency);
  }

  @Override
  public Context tryBegin(Dependency dependency) {
    ensureState(State.STANDBY);
    lock = dependencyLock(dependency);
    boolean result = lock.tryLock();
    if (result) {
      return prepare(dependency);
    } else {
      lock = null;
      return null;
    }
  }

  private Context prepare(Dependency dependency) {
    this.context = new BaseContext(dependency);
    this.state = State.RUNNING;
    this.dependency = dependency;
    return this.context;
  }

  @Override
  public void commit() {
    ensureState(State.RUNNING);
    // Write in a multi persistence transaction
    MultiPersistenceTransaction multiPersistenceTransaction = new MultiPersistenceTransaction();
    for (Variable variable : dependency.writes()) {
      // Only write variables that have been written to the context
      if (context.written(variable)) {
        Object value = context.read(variable);
        variable.driver().persistence().writeInMultiTransaction(multiPersistenceTransaction, value);
      }
    }
    // Commit multi persistence transaction
    multiPersistenceTransaction.commit();
    // Release execution locks
    lock.unlock();
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    ensureState(State.RUNNING);
    lock.unlock();
    state = State.ABORTED;
  }

  private static Lock dependencyLock(Dependency dependency) {
    return new MultiLock(orderedLocks(dependency));
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

  private void ensureState(State state) {
    if (this.state != state) {
      throw new IllegalStateException("State should be " + state + " but is " + this.state);
    }
  }
}
