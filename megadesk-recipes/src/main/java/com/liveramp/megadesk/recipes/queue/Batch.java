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
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Batch<VALUE> extends BaseQueue<VALUE, ImmutableList<VALUE>> {

  public Batch(Variable<ImmutableList<VALUE>> input, Variable<ImmutableList<VALUE>> output, Variable<Boolean> frozen) {
    super(input, output, frozen);
  }

  @Override
  protected ImmutableList<VALUE> internalRead(ImmutableList<VALUE> transfer) {
    return transfer;
  }

  @Override
  protected Transaction getPopTransaction() {
    return new Erase<VALUE>(getOutput(), getFrozen());
  }
}

