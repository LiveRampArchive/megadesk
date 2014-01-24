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

package com.liveramp.megadesk.recipes.batch;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Batch<VALUE> {

  private final Driver<ImmutableList> input;
  private final Driver<ImmutableList> output;
  private Driver<Boolean> frozen;

  public Batch(Driver<ImmutableList> input, Driver<ImmutableList> output, Driver<Boolean> frozen) {
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

  protected Append<VALUE> getAppendTransaction(VALUE value) {
    return new Append<VALUE>(input, value);
  }

  public ImmutableList<VALUE> readBatch(Context context) {
    TransferBatch transferBatch = getTransferTransaction();
    try {
      transferBatch.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ImmutableList batch = context.read(output.reference());
    return batch;
  }

  protected TransferBatch getTransferTransaction() {
    return new TransferBatch(input, output, frozen);
  }

  public void popBatch(Context context) {
    Erase erase = getEraseTransaction();
    try {
      erase.run(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Erase getEraseTransaction() {
    return new Erase(output, frozen);
  }

  private static class TransferBatch implements Transaction<Void> {

    private final Dependency dependency;
    private final Driver<ImmutableList> input;
    private final Driver<ImmutableList> output;
    private final Driver<Boolean> frozen;

    public TransferBatch(Driver<ImmutableList> input, Driver<ImmutableList> output, Driver<Boolean> frozen) {
      this.input = input;
      this.output = output;
      this.frozen = frozen;
      this.dependency = BaseDependency.builder().writes(input, output, frozen).build();
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      Binding<ImmutableList> inputList = context.binding(input.reference());
      Binding<ImmutableList> outputList = context.binding(output.reference());
      Binding<Boolean> frozenFlag = context.binding(frozen.reference());
      if (!frozenFlag.read()) {
        if (!outputList.read().isEmpty()) {
          throw new IllegalStateException("Batch should not be unfrozen when output still remains!");
        }
        ImmutableList values = inputList.read();
        outputList.write(values);
        inputList.write(ImmutableList.of());
        frozenFlag.write(true);
      }
      return null;
    }
  }

  private static class Append<V> implements Transaction<Void> {

    private final Dependency dependency;
    private final Reference<ImmutableList> reference;
    private final V value;

    private Append(Driver<ImmutableList> driver, V value) {
      this.value = value;
      this.reference = driver.reference();
      this.dependency = BaseDependency.builder().writes(driver).build();
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      ImmutableList originalValue = context.read(reference);
      ImmutableList newValue = ImmutableList.builder().addAll(originalValue).add(value).build();
      context.write(reference, newValue);
      return null;
    }
  }

  private static class Erase implements Transaction<Void> {

    private final Dependency dependency;
    private final Reference<ImmutableList> listReference;
    private final Reference<Boolean> frozen;

    private Erase(Driver<ImmutableList> listDriver, Driver<Boolean> frozen) {
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
}

