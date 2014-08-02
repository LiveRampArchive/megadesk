package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.transaction.Read;
import com.liveramp.megadesk.recipes.transaction.Write;

public class UnsafeQueue<VALUE> {

  private final Variable<ImmutableList<VALUE>> input;
  private final Variable<ImmutableList<VALUE>> output;
  private final TransactionExecutor executor;

  public UnsafeQueue(TransactionExecutor executor, Variable<ImmutableList<VALUE>> input, Variable<ImmutableList<VALUE>> output) {
    this.input = input;
    this.output = output;
    this.executor = executor;
  }

  public ImmutableList<VALUE> readInput() {
    try {
      return executor.execute(getReadTransaction(input));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ImmutableList<VALUE> readOutput() {
    try {
      return executor.execute(getReadTransaction(output));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeInput(ImmutableList<VALUE> values) {
    try {
      executor.execute(getWriteTransaction(input, values));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeOutput(ImmutableList<VALUE> values) {
    try {
      executor.execute(getWriteTransaction(output, values));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Read<ImmutableList<VALUE>> getReadTransaction(Variable<ImmutableList<VALUE>> variable) {
    return new Read<ImmutableList<VALUE>>(variable);
  }

  private Write<ImmutableList<VALUE>> getWriteTransaction(Variable<ImmutableList<VALUE>> variable, ImmutableList<VALUE> values) {
    return new Write<ImmutableList<VALUE>>(variable, values);
  }
}
