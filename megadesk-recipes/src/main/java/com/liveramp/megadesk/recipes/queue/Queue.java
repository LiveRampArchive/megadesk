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

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Queue<VALUE> extends BaseQueue<VALUE, VALUE> {

  public Queue(Variable<ImmutableList> input, Variable<ImmutableList> output, Variable<Boolean> frozen) {
    super(input, output, frozen);
  }

  @Override
  protected VALUE internalRead(ImmutableList<VALUE> transfer) {
    if (transfer.isEmpty()) {
      return null;
    } else {
      return transfer.get(0);
    }
  }

  @Override
  protected Transaction getPopTransaction() {
    return new PopOne(this.getOutput(), this.getFrozen());
  }
}
