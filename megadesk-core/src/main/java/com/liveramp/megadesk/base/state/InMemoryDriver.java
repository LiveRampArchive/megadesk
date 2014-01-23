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

package com.liveramp.megadesk.base.state;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Persistence;
import com.liveramp.megadesk.core.state.ReadWriteLock;
import com.liveramp.megadesk.core.state.Reference;

public class InMemoryDriver<VALUE> implements Driver<VALUE> {

  private final Reference<VALUE> reference = new InMemoryReference<VALUE>();
  private final ReadWriteLock executionLock = new InMemoryReadWriteLock();
  private final ReadWriteLock persistenceLock = new InMemoryReadWriteLock();
  private final Persistence<VALUE> persistence;

  public InMemoryDriver() {
    persistence = new InMemoryPersistence<VALUE>();
  }

  public InMemoryDriver(VALUE value) {
    persistence = new InMemoryPersistence<VALUE>(value);
  }

  @Override
  public Reference<VALUE> reference() {
    return reference;
  }

  @Override
  public ReadWriteLock executionLock() {
    return executionLock;
  }

  @Override
  public ReadWriteLock persistenceLock() {
    return persistenceLock;
  }

  @Override
  public Persistence<VALUE> persistence() {
    return persistence;
  }

  @Override
  public int compareTo(Driver<VALUE> other) {
    return new CompareToBuilder().append(reference, other.reference()).toComparison();
  }
}
