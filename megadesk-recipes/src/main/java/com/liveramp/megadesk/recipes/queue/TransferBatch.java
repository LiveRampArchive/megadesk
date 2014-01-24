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
import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

class TransferBatch implements Transaction<ImmutableList> {

  private final Dependency dependency;
  private final Variable<ImmutableList> input;
  private final Variable<ImmutableList> output;
  private final Variable<Boolean> frozen;

  public TransferBatch(Driver<ImmutableList> inputDriver, Driver<ImmutableList> outputDriver, Driver<Boolean> frozenDriver) {
    this.input = new Local<ImmutableList>(inputDriver);
    this.output = new Local<ImmutableList>(outputDriver);
    this.frozen = new Local<Boolean>(frozenDriver);
    this.dependency = BaseDependency.builder().writes(input, output, frozen).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public ImmutableList run(Context context) throws Exception {
    Accessor<ImmutableList> inputList = context.accessor(input.reference());
    Accessor<ImmutableList> outputList = context.accessor(output.reference());
    Accessor<Boolean> frozenFlag = context.accessor(frozen.reference());
    if (!frozenFlag.read()) {
      if (!outputList.read().isEmpty()) {
        throw new IllegalStateException("Batch should not be unfrozen when output still remains!");
      }
      ImmutableList values = inputList.read();
      outputList.write(values);
      inputList.write(ImmutableList.of());
      frozenFlag.write(true);
    }
    return outputList.read();
  }
}
