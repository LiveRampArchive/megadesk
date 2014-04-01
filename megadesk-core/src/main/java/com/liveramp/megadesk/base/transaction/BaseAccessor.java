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

import java.util.Arrays;

import com.liveramp.megadesk.base.state.InMemoryPersistence;
import com.liveramp.megadesk.core.state.Persistence;
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.DependencyType;
import com.liveramp.megadesk.utils.FormatUtils;

public class BaseAccessor<VALUE> implements Accessor<VALUE> {

  private final Persistence<VALUE> persistence;
  private final DependencyType dependencyType;

  public BaseAccessor(VALUE value, DependencyType dependencyType) {
    this.dependencyType = dependencyType;
    this.persistence = new InMemoryPersistence<VALUE>(value);
  }

  @Override
  public VALUE read() {
    ensureDependencyType(DependencyType.READ, DependencyType.WRITE);
    return persistence.read();
  }

  @Override
  public void write(VALUE value) {
    ensureDependencyType(DependencyType.WRITE);
    persistence.write(value);
  }

  private void ensureDependencyType(DependencyType... dependencyTypes) {
    if (!Arrays.asList(dependencyTypes).contains(dependencyType)) {
      throw new IllegalArgumentException(); // TODO message
    }
  }

  @Override
  public String toString() {
    return FormatUtils.formatToString(this, dependencyType.toString());
  }
}
