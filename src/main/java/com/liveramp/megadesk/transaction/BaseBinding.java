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

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.lib.InMemoryPersistence;

public class BaseBinding<VALUE> implements Binding<VALUE> {

  private final Persistence<VALUE> persistence;
  private final boolean readOnly;

  public BaseBinding(Value<VALUE> value, boolean readOnly) {
    this.readOnly = readOnly;
    persistence = new InMemoryPersistence<VALUE>(value);
  }

  @Override
  public Value<VALUE> read() {
    return persistence.read();
  }

  @Override
  public VALUE get() {
    return persistence.get();
  }

  @Override
  public void write(Value<VALUE> value) {
    if (readOnly) {
      throw new IllegalStateException(); // TODO message
    }
    persistence.write(value);
  }
}
