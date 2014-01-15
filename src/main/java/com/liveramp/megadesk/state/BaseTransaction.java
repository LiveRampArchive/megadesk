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

package com.liveramp.megadesk.state;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class BaseTransaction implements Transaction, TransactionExecution, TransactionData {

  public enum State {
    STANDBY,
    RUNNING,
    COMMITTED,
    ABORTED
  }

  private State state = State.STANDBY;
  private List<Reference> reads;
  private List<Reference> writes;
  private final Map<Reference, Object> updates;

  public BaseTransaction() {
    this.reads = Collections.emptyList();
    this.writes = Collections.emptyList();
    updates = Maps.newHashMap();
  }

  public BaseTransaction(List<Reference> reads,
                         List<Reference> writes) {
    this.reads = reads;
    this.writes = writes;
    updates = Maps.newHashMap();
  }

  @Override
  public TransactionExecution execution() {
    return this;
  }

  @Override
  public TransactionData data() {
    return this;
  }

  public BaseTransaction reads(Reference... references) {
    return reads(Arrays.asList(references));
  }

  public BaseTransaction reads(List<Reference> references) {
    ensureState(State.STANDBY);
    this.reads = references;
    return this;
  }

  public BaseTransaction writes(Reference... references) {
    return writes(Arrays.asList(references));
  }

  public BaseTransaction writes(List<Reference> references) {
    ensureState(State.STANDBY);
    this.writes = references;
    return this;
  }

  @Override
  public void begin() {
    ensureState(State.STANDBY);
    lock();
    state = State.RUNNING;
  }

  @Override
  public boolean tryBegin() {
    ensureState(State.STANDBY);
    boolean result = tryLock();
    if (result) {
      state = State.RUNNING;
    }
    return result;
  }

  @Override
  public <VALUE> Value<VALUE> read(Reference<VALUE> reference) {
    ensureState(State.RUNNING);
    if (!reads.contains(reference)) {
      throw new IllegalArgumentException(); // TODO message
    }
    if (updates.containsKey(reference)) {
      return (Value<VALUE>)updates.get(reference);
    } else {
      return reference.read();
    }
  }

  @Override
  public <VALUE> void write(Reference<VALUE> reference, Value<VALUE> value) {
    ensureState(State.RUNNING);
    if (!writes.contains(reference)) {
      throw new IllegalArgumentException(); // TODO message
    }
    updates.put(reference, value);
  }

  @Override
  public void commit() {
    ensureState(State.RUNNING);
    // Write updates
    for (Map.Entry<Reference, Object> entry : updates.entrySet()) {
      Value value = (Value)entry.getValue();
      entry.getKey().write(value);
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

  private boolean tryLock() {
    for (Reference read : reads) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (writes.contains(read)) {
        continue;
      }
      if (!read.lock().readLock().tryLock()) {
        unlock();
        return false;
      }
    }
    for (Reference write : writes) {
      if (!write.lock().writeLock().tryLock()) {
        unlock();
        return false;
      }
    }
    return true;
  }

  private void lock() {
    for (Reference read : reads) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (writes.contains(read)) {
        continue;
      }
      read.lock().readLock().lock();
    }
    for (Reference write : writes) {
      write.lock().writeLock().lock();
    }
  }

  private void unlock() {
    for (Reference read : reads) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (writes.contains(read)) {
        continue;
      }
      read.lock().readLock().unlock();
    }
    for (Reference write : writes) {
      write.lock().writeLock().unlock();
    }
  }

  private void ensureState(State state) {
    if (this.state != state) {
      throw new IllegalArgumentException("Transaction state should be " + state + " but is " + this.state);
    }
  }
}
