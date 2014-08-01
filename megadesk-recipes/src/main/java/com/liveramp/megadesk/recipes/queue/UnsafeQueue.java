package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.transaction.Read;
import com.liveramp.megadesk.recipes.transaction.Write;

public class UnsafeQueue<VALUE> {

  private final Variable<ImmutableList<VALUE>> input;
  private final Variable<ImmutableList<VALUE>> output;

  public UnsafeQueue(Variable<ImmutableList<VALUE>> input, Variable<ImmutableList<VALUE>> output) {
    this.input = input;
    this.output = output;
  }

  public ImmutableList<VALUE> readInput(TransactionExecutor executor) {

    try {
      return executor.execute(getReadTransaction(input));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ImmutableList<VALUE> readOutput(TransactionExecutor executor) {

    try {
      return executor.execute(getReadTransaction(output));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeInput(TransactionExecutor executor, ImmutableList<VALUE> values) {

    try {
      executor.execute(getWriteTransaction(input, values));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeOutput(TransactionExecutor executor, ImmutableList<VALUE> values) {

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
