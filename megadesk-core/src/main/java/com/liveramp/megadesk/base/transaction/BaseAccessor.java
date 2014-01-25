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

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.state.InMemoryPersistence;
import com.liveramp.megadesk.core.state.Persistence;
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.Commutation;

public class BaseAccessor<VALUE> implements Accessor<VALUE> {

  private final Persistence<VALUE> persistence;
  private final List<Commutation> commutations;
  private final boolean readOnly;

  public BaseAccessor(VALUE value, boolean readOnly) {
    this.readOnly = readOnly;
    this.persistence = new InMemoryPersistence<VALUE>(value);
    this.commutations = Lists.newArrayList();
  }

  @Override
  public VALUE read() {
    // TODO: check permissions
    return persistence.read();
  }

  @Override
  public void write(VALUE value) {
    // TODO: check permissions
    if (readOnly) {
      throw new IllegalStateException(); // TODO message
    }
    persistence.write(value);
  }

  @Override
  public VALUE commute(Commutation<VALUE> commutation) {
    // TODO: check permissions
    write(commutation.commute(read()));
    commutations.add(commutation);
    return read();
  }

  @Override
  public List<Commutation> commutations() {
    // TODO: check permissions
    return commutations;
  }
}
