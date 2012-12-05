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

import java.util.Collections;
import java.util.Set;

public abstract class BaseDependency<RESOURCE> implements Dependency {

  private final Resource<RESOURCE> resource;

  public BaseDependency(Resource<RESOURCE> resource) {
    this.resource = resource;
  }

  @Override
  public Set<Resource> getResources() {
    return Collections.singleton((Resource) resource);
  }

  @Override
  public boolean check(ConditionWatcher watcher) throws Exception {
    return check(resource.read(watcher));
  }

  public abstract boolean check(RESOURCE resourceData) throws Exception;
}
