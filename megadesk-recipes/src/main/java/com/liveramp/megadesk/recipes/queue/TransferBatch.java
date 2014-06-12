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
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Transaction;

public class TransferBatch<VALUE> extends BaseTransaction<ImmutableList<VALUE>> implements Transaction<ImmutableList<VALUE>> {

  private final Variable<ImmutableList<VALUE>> input;
  private final Variable<ImmutableList<VALUE>> output;
  private final Variable<Boolean> frozen;

  public TransferBatch(Variable<ImmutableList<VALUE>> input, Variable<ImmutableList<VALUE>> output, Variable<Boolean> frozen) {
    super(BaseDependency.builder().writes(input, output, frozen).build());
    this.input = input;
    this.output = output;
    this.frozen = frozen;
  }

  @Override
  public ImmutableList<VALUE> run(Context context) throws Exception {
    Accessor<ImmutableList<VALUE>> inputList = context.accessor(input);
    Accessor<ImmutableList<VALUE>> outputList = context.accessor(output);
    Accessor<Boolean> frozenFlag = context.accessor(frozen);
    if (!frozenFlag.read()) {
      if (!outputList.read().isEmpty()) {
        throw new IllegalStateException("Batch should not be unfrozen when output still remains!");
      }
      ImmutableList<VALUE> values = inputList.read();
      outputList.write(values);
      inputList.write(ImmutableList.<VALUE>of());
      frozenFlag.write(true);
    }
    return outputList.read();
  }
}
