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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.lock.Lock;
import com.liveramp.megadesk.persistence.Persistence;

public abstract class BaseGear implements Gear {

  private List<Dependency> dependencies;
  private final Lock masterLock;

  public BaseGear(Lock masterLock) {
    this.dependencies = Collections.emptyList();
    this.masterLock = masterLock;
  }

  @Override
  public Lock getMasterLock() {
    return masterLock;
  }

  @Override
  public Persistence getPersistence() {
    return null;
  }

  @Override
  public List<Dependency> dependencies() {
    return dependencies;
  }

  public BaseGear depends(Dependency... dependencies) {
    return depends(Arrays.asList(dependencies));
  }

  public BaseGear depends(List<Dependency> dependencies) {
    this.dependencies = dependencies;
    return this;
  }

  @Override
  public String toString() {
    return "[" + BaseGear.class.getSimpleName() + " " + getNode().getPath().get() + "]";
  }
}
