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

import com.google.common.collect.Maps;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;

public class BaseTransactionData implements TransactionData {

  private final Map<Reference, Binding> bindings;

  public BaseTransactionData(TransactionDependency dependency) {
    bindings = Maps.newHashMap();
    for (Driver driver : dependency.reads()) {
      // TODO is this necessary?
      // Skip to avoid deadlocks
      if (dependency.writes().contains(driver)) {
        continue;
      }
      bindings.put(driver.reference(), new BaseBinding(driver.persistence().read(), true));
    }
    for (Driver driver : dependency.writes()) {
      bindings.put(driver.reference(), new BaseBinding(driver.persistence().read(), false));
    }
  }

  @Override
  public <VALUE> Binding<VALUE> get(Reference<VALUE> reference) {
    if (!bindings.containsKey(reference)) {
      throw new IllegalStateException(); // TODO message
    }
    return bindings.get(reference);
  }
}
