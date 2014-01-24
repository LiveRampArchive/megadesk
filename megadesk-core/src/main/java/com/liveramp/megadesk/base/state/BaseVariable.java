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

import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.state.Variable;

public class BaseVariable<VALUE> implements Variable<VALUE> {

  private final Reference<VALUE> reference;
  private final Driver<VALUE> driver;

  public BaseVariable(Reference<VALUE> reference, Driver<VALUE> driver) {
    this.reference = reference;
    this.driver = driver;
  }

  @Override
  public Reference<VALUE> reference() {
    return reference;
  }

  @Override
  public Driver<VALUE> driver() {
    return driver;
  }

  @Override
  public int compareTo(Variable<VALUE> o) {
    return reference.compareTo(o.reference());
  }

  @Override
  public String toString() {
    return this.reference().name();
  }
}
