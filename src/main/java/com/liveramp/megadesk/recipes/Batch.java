package com.liveramp.megadesk.recipes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.lib.InMemoryDriver;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.Binding;
import com.liveramp.megadesk.transaction.Dependency;
import com.liveramp.megadesk.transaction.Procedure;
import com.liveramp.megadesk.transaction.Transaction;
import com.liveramp.megadesk.utils.ValueWrapper;

import java.util.Map;

public class Batch<VALUE> {

  private static Map<String, Driver<ImmutableList>> drivers = Maps.newConcurrentMap();

  private final Driver<ImmutableList> input;
  private final Driver<ImmutableList> output;
  private static ValueWrapper wrapper = new InMemoryWrapper();

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
    return new InMemoryDriver<ImmutableList>(wrapper.<ImmutableList>wrap(ImmutableList.of()));
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
    ImmutableList batch = read(output);
    return batch;
  }

  private <T> T read(Driver<T> ref) {
    return ref.persistence().get();
  }

  public void popBatch() {
    try {
      new BaseExecutor().execute(new Erase(output));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TransferBatch implements Procedure {

    private final Dependency dependency;
    private final Driver<ImmutableList> input;
    private final Driver<ImmutableList> output;

    public TransferBatch(Driver<ImmutableList> input, Driver<ImmutableList> output) {
      this.input = input;
      this.output = output;
      this.dependency = BaseDependency.builder().writes(input, output).build();
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    @Override
    public void run(Transaction transaction) throws Exception {
      Binding<ImmutableList> inputList = transaction.binding(input.reference());
      Binding<ImmutableList> outputList = transaction.binding(output.reference());
      if (outputList.get().isEmpty()) {
        Value<ImmutableList> values = inputList.read();
        outputList.write(values);
        inputList.write(wrapper.<ImmutableList>wrap(ImmutableList.of()));
      }
    }
  }

  private static class Append<V> implements Procedure {

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
    public void run(Transaction transaction) throws Exception {
      Value<ImmutableList> originalValue = transaction.read(reference);
      ImmutableList originalList = originalValue.get();
      ImmutableList newList = ImmutableList.<V>builder().addAll(originalList).add(value).build();
      Value<ImmutableList> newValue = wrapper.wrap(newList);
      transaction.write(reference, newValue);
    }
  }

  private static class Erase implements Procedure {

    private final Dependency dependency;
    private final Reference<ImmutableList> reference;

    private Erase(Driver<ImmutableList> driver) {
      this.reference = driver.reference();
      this.dependency = BaseDependency.builder().writes(driver).build();
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    @Override
    public void run(Transaction transaction) throws Exception {
      transaction.write(reference, wrapper.<ImmutableList>wrap(ImmutableList.of()));
    }
  }

  private static class InMemoryWrapper implements ValueWrapper {

    @Override
    public <T> Value<T> wrap(T value) {
      return new InMemoryValue<T>(value);
    }
  }
}
