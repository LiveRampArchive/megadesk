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

package com.liveramp.megadesk.resource.lib;

import com.liveramp.megadesk.Megadesk;
import com.liveramp.megadesk.dependency.BaseDependency;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.dependency.lib.ComparisonDependency;
import com.liveramp.megadesk.resource.BaseResource;
import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.serialization.lib.IntegerSerialization;

public class IntegerResource extends BaseResource<Integer> implements Resource<Integer> {

  public IntegerResource(Megadesk megadesk, String id) throws Exception {
    super(id, megadesk.getResourceDriver(id), new IntegerSerialization());
  }

  public Read equals(Integer version) {
    return new Read<Integer>(this, new ComparisonDependency<Integer>(version, 0));
  }

  public Read lessThan(final Integer version) {
    return new Read<Integer>(this, new ComparisonDependency<Integer>(version, -1));
  }

  public Read greaterThan(final Integer version) {
    return new Read<Integer>(this, new ComparisonDependency<Integer>(version, 1));
  }

  public static Dependency<Integer, Integer> GREATER_THAN_STEP = new BaseDependency<Integer, Integer>() {
    @Override
    public boolean check(Integer stepData, Integer resourceData) throws Exception {
      return resourceData != null && (stepData == null || resourceData > stepData);
    }
  };
}
