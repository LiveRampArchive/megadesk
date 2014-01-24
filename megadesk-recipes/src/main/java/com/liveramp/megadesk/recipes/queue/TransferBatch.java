package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

class TransferBatch implements Transaction<Void> {

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
