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

import java.util.Map;

import com.google.common.collect.Maps;

public class BaseTransactionData implements TransactionData {

  private final TransactionDependency dependency;
  private final Map<Reference, Value> updates;

  public BaseTransactionData(TransactionDependency dependency) {
    this.dependency = dependency;
    this.updates = Maps.newHashMap();
  }

  @Override
  public <VALUE> Value<VALUE> read(Reference<VALUE> reference) {
    if (!dependency.reads().contains(reference)) {
      throw new IllegalArgumentException(reference + " is not a read dependency");
    }
    if (updates.containsKey(reference)) {
      return (Value<VALUE>)updates.get(reference);
    } else {
      return reference.read();
    }
  }

  @Override
  public <VALUE> void write(Reference<VALUE> reference, Value<VALUE> value) {
    if (!dependency.writes().contains(reference)) {
      throw new IllegalArgumentException(reference + " is not a write dependency");
    }
    updates.put(reference, value);
  }

  @Override
  public Map<Reference, Value> updates() {
    return updates;
  }
}
