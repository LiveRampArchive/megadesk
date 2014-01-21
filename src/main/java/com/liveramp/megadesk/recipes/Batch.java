package com.liveramp.megadesk.recipes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.liveramp.megadesk.gear.BaseGear;
import com.liveramp.megadesk.gear.Outcome;
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
import com.liveramp.megadesk.worker.NaiveWorker;
import com.liveramp.megadesk.worker.Worker;

import java.util.List;
import java.util.Map;

public class Batch<VALUE, MergedValues> {

  private static Map<String, Driver<ImmutableList>> drivers = Maps.newConcurrentMap();

  private final Driver<ImmutableList> input;
  private final Driver<ImmutableList> output;
  private final Merger<VALUE, MergedValues> merger;
  private static ValueWrapper wrapper = new InMemoryWrapper();

  public static <VALUE, MergedValues> Batch<VALUE, MergedValues> getByName(String name, Merger<VALUE, MergedValues> merger) {
    return new Batch<VALUE, MergedValues>(getDriver(name + "-input"), getDriver(name + "-output"), merger);
  }

  public Batch(Driver<ImmutableList> input, Driver<ImmutableList> output, Merger<VALUE, MergedValues> merger) {
    this.input = input;
    this.output = output;
    this.merger = merger;
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

  private Worker makeWorker() {
    return new NaiveWorker();
  }

  public void append(VALUE value) {
    Append<VALUE> append = new Append<VALUE>(input, value);
    try {
      new BaseExecutor().execute(append);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public MergedValues readBatch() {
    try {
      new BaseExecutor().execute(new BatcherGear());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ImmutableList batch = read(output);
    return merger.merge(batch);
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

  private class BatcherGear extends BaseGear {

    public BatcherGear() {
      super(BaseDependency.builder().writes(input, output).build());
    }

    @Override
    public Value<Outcome> call(Transaction transaction) throws Exception {
      Binding<ImmutableList> inputList = transaction.binding(input.reference());
      Binding<ImmutableList> outputList = transaction.binding(output.reference());
      if (outputList.get().isEmpty()) {
        Value<ImmutableList> values = inputList.read();
        outputList.write(values);
        inputList.write(wrapper.<ImmutableList>wrap(ImmutableList.of()));
      }
      return wrapper.wrap(Outcome.SUCCESS);
    }
  }

  private class Append<V> implements Procedure {

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

  private class Erase implements Procedure {

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

  public static interface Merger<VALUE, MergedValues> {

    public MergedValues merge(List<VALUE> values);
  }

  private static class InMemoryWrapper implements ValueWrapper {

    @Override
    public <T> Value<T> wrap(T value) {
      return new InMemoryValue<T>(value);
    }
  }
}
