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
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public abstract class BaseQueue<VALUE, OUTPUT> {

  private final Variable<ImmutableList> input;
  private final Variable<ImmutableList> output;
  private final Variable<Boolean> frozen;

  public BaseQueue(Variable<ImmutableList> input, Variable<ImmutableList> output, Variable<Boolean> frozen) {
    this.input = input;
    this.output = output;
    this.frozen = frozen;
  }

  public Dependency getAppendDependency() {
    return BaseDependency.builder().writes(input).build();
  }

  public Dependency getPopDependency() {
    return BaseDependency.builder().writes(input, output, frozen).build();
  }

  public void append(Context context, VALUE... values) {
    Append<VALUE> append = getAppendTransaction(values);
    try {
      append.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Variable<ImmutableList> getInput() {
    return input;
  }

  public Variable<ImmutableList> getOutput() {
    return output;
  }

  public Variable<Boolean> getFrozen() {
    return frozen;
  }

  protected Append<VALUE> getAppendTransaction(VALUE... values) {
    return new Append<VALUE>(input, values);
  }

  protected ImmutableList<VALUE> transfer(Context context) {
    TransferBatch transferBatch = getTransferTransaction();
    try {
      return transferBatch.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected TransferBatch getTransferTransaction() {
    return new TransferBatch(input, output, frozen);
  }

  public void pop(Context context) {
    Transaction pop = getPopTransaction();
    try {
      pop.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public OUTPUT read(Context context) {
    ImmutableList<VALUE> transfer = transfer(context);
    return internalRead(transfer);
  }

  protected abstract OUTPUT internalRead(ImmutableList<VALUE> transfer);

  protected abstract Transaction getPopTransaction();
}

