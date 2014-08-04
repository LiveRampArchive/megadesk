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
    return read(input);
  }

  public ImmutableList<VALUE> readOutput() {
    return read(output);
  }

  public void writeInput(ImmutableList<VALUE> values) {
    write(input, values);
  }

  public void writeOutput(ImmutableList<VALUE> values) {
    write(output, values);
  }

  private ImmutableList<VALUE> read(Variable<ImmutableList<VALUE>> variable) {
    try {
      return executor.execute(getReadTransaction(variable));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void write(Variable<ImmutableList<VALUE>> variable, ImmutableList<VALUE> values) {
    try {
      executor.execute(getWriteTransaction(variable, values));
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
