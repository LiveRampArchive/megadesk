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

package com.liveramp.megadesk.dependency;

import java.util.Arrays;
import java.util.List;

import com.liveramp.megadesk.register.Participant;

public class MultiDependency implements Dependency {

  private final List<Dependency> dependencies;

  public MultiDependency(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }

  public MultiDependency(Dependency... dependencies) {
    this.dependencies = Arrays.asList(dependencies);
  }

  @Override
  public DependencyCheck acquire(Participant participant) throws Exception {
    DependencyCheck result = DependencyCheck.ACQUIRED;
    for (Dependency dependency : dependencies) {
      DependencyCheck check = dependency.acquire(participant);
      if (check != DependencyCheck.ACQUIRED) {
        if (result == DependencyCheck.ACQUIRED || result == DependencyCheck.STANDBY) {
          result = check;
        }
      }
    }
    if (result != DependencyCheck.ACQUIRED) {
      for (Dependency dependency : dependencies) {
        dependency.release(participant);
      }
    }
    return result;
  }

  @Override
  public void release(Participant participant) throws Exception {
    for (Dependency dependency : dependencies) {
      dependency.release(participant);
    }
  }
}
