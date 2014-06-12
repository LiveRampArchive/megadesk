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

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Append<VALUE> extends BaseTransaction<Void> implements Transaction<Void> {

  private final Variable<ImmutableList<VALUE>> list;
  private final List<VALUE> values;

  public Append(Variable<ImmutableList<VALUE>> driver, List<VALUE> values) {
    super(BaseDependency.builder().writes(driver).build());
    this.values = values;
    this.list = driver;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList<VALUE> originalValue = context.read(list);
    ImmutableList<VALUE> newValue = ImmutableList.<VALUE>builder().addAll(originalValue).addAll(values).build();
    context.write(list, newValue);
    return null;
  }
}
