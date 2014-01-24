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
package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;

import com.liveramp.megadesk.core.transaction.Transaction;

class Erase implements Transaction<Void> {

  private final Dependency dependency;
  private final Reference<ImmutableList> listReference;
  private final Reference<Boolean> frozen;

  Erase(Driver<ImmutableList> listDriver, Driver<Boolean> frozen) {
    this.frozen = frozen.reference();
    this.listReference = listDriver.reference();
    this.dependency = BaseDependency.builder().writes(listDriver, frozen).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    context.write(listReference, ImmutableList.of());
    context.write(frozen, false);
    return null;
  }
}
