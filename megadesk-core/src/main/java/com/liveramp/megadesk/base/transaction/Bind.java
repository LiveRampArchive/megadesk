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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.liveramp.megadesk.base.state.Name;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Binding;

public class Bind implements Binding {

  private final Map<Reference, Driver> binding;

  public Bind() {
    this(Maps.<Reference, Driver>newHashMap());
  }

  public <VALUE> Bind(Reference<VALUE> reference, Driver<VALUE> driver) {
    this(ImmutableMap.<Reference, Driver>of(reference, driver));
  }

  public <VALUE> Bind(Reference<VALUE> reference, Variable<VALUE> variable) {
    this(ImmutableMap.<Reference, Driver>of(reference, variable.driver()));
  }

  public <VALUE> Bind(String name, Driver<VALUE> driver) {
    this(ImmutableMap.<Reference, Driver>of(new Name<VALUE>(name), driver));
  }

  public <VALUE> Bind(String name, Variable<VALUE> variable) {
    this(ImmutableMap.<Reference, Driver>of(new Name<VALUE>(name), variable.driver()));
  }

  public Bind(Map<Reference, Driver> binding) {
    this.binding = Collections.unmodifiableMap(Maps.newHashMap(binding));
  }

  @Override
  public Driver get(Reference reference) {
    if (!binding.containsKey(reference)) {
      throw new IllegalStateException(); // TODO message
    }
    return binding.get(reference);
  }

  public <VALUE> Bind bind(Reference<VALUE> reference, Driver<VALUE> driver) {
    Map<Reference, Driver> binding = Maps.newHashMap();
    binding.put(reference, driver);
    return new Bind(binding);
  }

  public <VALUE> Bind bind(Reference<VALUE> reference, Variable<VALUE> variable) {
    return bind(reference, variable.driver());
  }

  public <VALUE> Bind bind(String name, Driver<VALUE> driver) {
    return bind(new Name<VALUE>(name), driver);
  }

  public <VALUE> Bind bind(String name, Variable<VALUE> variable) {
    return bind(new Name<VALUE>(name), variable);
  }
}
