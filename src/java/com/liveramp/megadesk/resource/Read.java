/**
 *  Copyright 2012 LiveRamp
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

package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.dependency.Dependency;

public class Read<T> {

  private final Resource<T> resource;
  private final Dependency<?, T> dependency;

  public Read(Resource<T> resource, Dependency<?, T> dependency) {
    this.resource = resource;
    this.dependency = dependency;
  }

  public Resource<T> getResource() {
    return resource;
  }

  public Dependency<?, T> getDependency() {
    return dependency;
  }
}
