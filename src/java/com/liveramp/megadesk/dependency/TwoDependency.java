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

package com.liveramp.megadesk.dependency;

import com.liveramp.megadesk.condition.ConditionWatcher;
import com.liveramp.megadesk.resource.Resource;

import java.util.HashSet;
import java.util.Set;

public abstract class TwoDependency<A, B> implements Dependency {

  private final Resource<A> resourceA;
  private final Resource<B> resourceB;

  private final Set<Resource> resources = new HashSet<Resource>();

  public TwoDependency(Resource<A> resourceA,
                       Resource<B> resourceB) {
    this.resourceA = resourceA;
    this.resourceB = resourceB;
    resources.add(resourceA);
    resources.add(resourceB);
  }

  @Override
  public Set<Resource> getResources() {
    return resources;
  }

  @Override
  public boolean check(ConditionWatcher watcher) throws Exception {
    return check(resourceA.read(watcher),
                 resourceB.read(watcher));
  }

  public abstract boolean check(A a,
                                B b) throws Exception;
}
