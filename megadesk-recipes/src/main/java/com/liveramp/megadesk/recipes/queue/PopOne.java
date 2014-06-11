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
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Transaction;

public class PopOne extends BaseTransaction<Void> implements Transaction<Void> {

  private final Variable<ImmutableList> list;
  private final Variable<Boolean> frozen;

  public PopOne(Variable<ImmutableList> list, Variable<Boolean> frozen) {
    super(BaseDependency.builder().writes(list, frozen).build());
    this.frozen = frozen;
    this.list = list;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList list = context.read(this.list);
    if (!list.isEmpty()) {
      ImmutableList newList = list.subList(1, list.size());
      context.write(this.list, newList);
    }
    if (context.read(this.list).isEmpty()) {
      context.write(this.frozen, false);
    }
    return null;
  }
}
