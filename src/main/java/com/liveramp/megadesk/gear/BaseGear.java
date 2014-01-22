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

package com.liveramp.megadesk.gear;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.transaction.Dependency;

public abstract class BaseGear implements Gear {

  private Dependency<Driver> dependency;

  public BaseGear() {
  }

  public BaseGear(Dependency<Driver> dependency) {
    this.dependency = dependency;
  }

  @Override
  public final Dependency<Driver> dependency() {
    if (dependency == null) {
      throw new IllegalStateException(); // TODO message
    }
    return dependency;
  }

  protected void setDependency(Dependency<Driver> dependency) {
    this.dependency = dependency;
  }
}
