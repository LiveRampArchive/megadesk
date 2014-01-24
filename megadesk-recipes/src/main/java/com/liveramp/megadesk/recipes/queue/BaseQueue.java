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
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Transaction;

public abstract class BaseQueue<VALUE> {

  private final Driver<ImmutableList> input;
  private final Driver<ImmutableList> output;
  private Driver<Boolean> frozen;

  public BaseQueue(Driver<ImmutableList> input, Driver<ImmutableList> output, Driver<Boolean> frozen) {
    this.input = input;
    this.output = output;
    this.frozen = frozen;
  }

  public void append(Context context, VALUE value) {
    Append<VALUE> append = getAppendTransaction(value);
    try {
      append.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Driver<ImmutableList> getInput() {
    return input;
  }

  public Driver<ImmutableList> getOutput() {
    return output;
  }

  public Driver<Boolean> getFrozen() {
    return frozen;
  }

  protected Append<VALUE> getAppendTransaction(VALUE value) {
    return new Append<VALUE>(input, value);
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

  protected abstract Transaction getPopTransaction();
}

