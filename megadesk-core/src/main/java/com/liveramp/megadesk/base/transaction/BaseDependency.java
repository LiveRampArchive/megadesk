package com.liveramp.megadesk.base.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.core.transaction.Dependency;

public class BaseDependency<V extends Comparable> implements Dependency<V> {

  private final List<V> snapshots;
  private final List<V> reads;
  private final List<V> writes;

  public BaseDependency(Collection<V> snapshots,
                        Collection<V> reads,
                        Collection<V> writes) {
    this.snapshots = prepareList(snapshots);
    this.reads = prepareList(reads);
    this.writes = prepareList(writes);
    checkIntegrity();
  }

  private List<V> prepareList(Collection<V> drivers) {
    // Deep copy, sort, and make unmodifiable
    List<V> result = Lists.newArrayList(drivers);
    Collections.sort(result);
    return Collections.unmodifiableList(result);
  }

  private void checkIntegrity() {
    for (V dependency : snapshots) {
      if (reads.contains(dependency) || writes.contains(dependency)) {
        throw new IllegalStateException(); // TODO message
      }
    }
    for (V dependency : reads) {
      if (writes.contains(dependency)) {
        throw new IllegalStateException(); // TODO message
      }
    }
  }

  public List<V> snapshots() {
    return snapshots;
  }

  public List<V> reads() {
    return reads;
  }

  public List<V> writes() {
    return writes;
  }

  public static class Builder<V extends Comparable> {

    private List<V> snapshots = Collections.emptyList();
    private List<V> reads = Collections.emptyList();
    private List<V> writes = Collections.emptyList();

    public Builder<V> snapshots(V... dependencies) {
      return snapshots(Arrays.asList(dependencies));
    }

    public Builder<V> snapshots(V[]... dependencies) {
      return snapshots(concatenate(dependencies));
    }

    public Builder<V> snapshots(List<V> dependencies) {
      this.snapshots = Lists.newArrayList(dependencies);
      return this;
    }

    public Builder<V> reads(V... dependencies) {
      return reads(Arrays.asList(dependencies));
    }

    public Builder<V> reads(V[]... dependencies) {
      return reads(concatenate(dependencies));
    }

    public Builder<V> reads(List<V> dependencies) {
      this.reads = Lists.newArrayList(dependencies);
      return this;
    }

    public Builder<V> writes(V... dependencies) {
      return writes(Arrays.asList(dependencies));
    }

    public Builder<V> writes(V[]... dependencies) {
      return writes(concatenate(dependencies));
    }

    public Builder<V> writes(List<V> drivers) {
      this.writes = Lists.newArrayList(drivers);
      return this;
    }

    private List<V> concatenate(V[]... lists) {
      List<V> result = Lists.newArrayList();
      for (V[] list : lists) {
        result.addAll(Arrays.asList(list));
      }
      return result;
    }

    public BaseDependency<V> build() {
      return new BaseDependency<V>(snapshots, reads, writes);
    }
  }

  public static <V extends Comparable> Builder<V> builder() {
    return new Builder<V>();
  }
}
