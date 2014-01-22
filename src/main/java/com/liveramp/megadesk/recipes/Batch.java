package com.liveramp.megadesk.recipes;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.lib.InMemoryDriver;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.Binding;
import com.liveramp.megadesk.transaction.Context;
import com.liveramp.megadesk.transaction.Dependency;
import com.liveramp.megadesk.transaction.Transaction;

public class Batch<VALUE> {

  private static Map<String, Driver<ImmutableList>> drivers = Maps.newConcurrentMap();

  private final Driver<ImmutableList> input;
  private final Driver<ImmutableList> output;

  public static <VALUE> Batch<VALUE> getByName(String name) {
    return new Batch<VALUE>(getDriver(name + "-input"), getDriver(name + "-output"));
  }

  public Batch(Driver<ImmutableList> input, Driver<ImmutableList> output) {
    this.input = input;
    this.output = output;
  }

  private static Driver<ImmutableList> getDriver(String name) {
    if (!drivers.containsKey(name)) {
      drivers.put(name, makeDriver(name));
    }
    return drivers.get(name);
  }

  private static Driver<ImmutableList> makeDriver(String name) {
    return new InMemoryDriver<ImmutableList>(ImmutableList.of());
  }

  public void append(VALUE value) {
    Append<VALUE> append = new Append<VALUE>(input, value);
    try {
      new BaseExecutor().execute(append);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ImmutableList<VALUE> readBatch() {
    try {
      new BaseExecutor().execute(new TransferBatch(input, output));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return read(output);
  }

  private <T> T read(Driver<T> ref) {
    return ref.persistence().read();
  }

  public void popBatch() {
    try {
      new BaseExecutor().execute(new Erase(output));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TransferBatch implements Transaction<Void> {

    private final Dependency<Driver> dependency;
    private final Driver<ImmutableList> input;
    private final Driver<ImmutableList> output;

    public TransferBatch(Driver<ImmutableList> input, Driver<ImmutableList> output) {
      this.input = input;
      this.output = output;
      this.dependency = BaseDependency.<Driver>builder().writes(input, output).build();
    }

    @Override
    public Dependency<Driver> dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      Binding<ImmutableList> inputList = context.binding(input.reference());
      Binding<ImmutableList> outputList = context.binding(output.reference());
      if (outputList.read().isEmpty()) {
        ImmutableList values = inputList.read();
        outputList.write(values);
        inputList.write(ImmutableList.of());
      }
      return null;
    }
  }

  private static class Append<V> implements Transaction<Void> {

    private final Dependency<Driver> dependency;
    private final Reference<ImmutableList> reference;
    private final V value;

    private Append(Driver<ImmutableList> driver, V value) {
      this.value = value;
      this.reference = driver.reference();
      this.dependency = BaseDependency.<Driver>builder().writes(driver).build();
    }

    @Override
    public Dependency<Driver> dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      ImmutableList originalList = context.read(reference);
      ImmutableList newList = ImmutableList.<V>builder().addAll(originalList).add(value).build();
      context.write(reference, newList);
      return null;
    }
  }

  private static class Erase implements Transaction<Void> {

    private final Dependency<Driver> dependency;
    private final Reference<ImmutableList> reference;

    private Erase(Driver<ImmutableList> driver) {
      this.reference = driver.reference();
      this.dependency = BaseDependency.<Driver>builder().writes(driver).build();
    }

    @Override
    public Dependency<Driver> dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      context.write(reference, ImmutableList.of());
      return null;
    }
  }
}
