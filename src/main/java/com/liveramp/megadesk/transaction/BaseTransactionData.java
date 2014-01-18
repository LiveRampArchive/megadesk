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

import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;

public class BaseTransactionData implements TransactionData {

  private final BaseTransactionDependency dependency;
  private final Map<Reference, Value> updates;

  public BaseTransactionData(BaseTransactionDependency dependency) {
    this.dependency = dependency;
    this.updates = Maps.newHashMap();
  }

  @Override
  public <VALUE> Value<VALUE> read(Reference<VALUE> reference) {
    if (!dependency.readReferences().contains(reference)) {
      throw new IllegalArgumentException(reference + " is not a read dependency");
    }
    if (updates.containsKey(reference)) {
      return (Value<VALUE>)updates.get(reference);
    } else {
      return dependency.readDriver(reference).persistence().read();
    }
  }

  @Override
  public <VALUE> VALUE get(Reference<VALUE> reference) {
    Value<VALUE> value = read(reference);
    if (value == null) {
      return null;
    } else {
      return value.get();
    }
  }

  @Override
  public <VALUE> void write(Reference<VALUE> reference, Value<VALUE> value) {
    if (!dependency.writeReferences().contains(reference)) {
      throw new IllegalArgumentException(reference + " is not a write dependency");
    }
    updates.put(reference, value);
  }

  public Map<Reference, Value> updates() {
    return updates;
  }
}
