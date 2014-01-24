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

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Binding;

public class BaseBinding implements Binding {

  private final Map<Reference, Driver> binding;

  public BaseBinding() {
    binding = Maps.newHashMap();
  }

  @Override
  public Driver get(Reference reference) {
    if (!binding.containsKey(reference)) {
      throw new IllegalStateException(); // TODO message
    }
    return binding.get(reference);
  }

  public <VALUE> void bind(Reference<VALUE> reference, Driver<VALUE> driver) {
    binding.put(reference, driver);
  }

  public <VALUE> void add(Variable<VALUE> variable) {
    binding.put(variable.reference(), variable.driver());
  }
}
