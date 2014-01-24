package com.liveramp.megadesk.base.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Dependency;

public class BaseDependency implements Dependency {

  private final List<Variable> snapshots;
  private final List<Variable> reads;
  private final List<Variable> writes;

  public BaseDependency(Collection<Variable> snapshots,
                        Collection<Variable> reads,
                        Collection<Variable> writes) {
    this.snapshots = prepareList(snapshots);
    this.reads = prepareList(reads);
    this.writes = prepareList(writes);
    checkIntegrity();
  }

  private List<Variable> prepareList(Collection<Variable> drivers) {
    // Deep copy, sort, and make unmodifiable
    List<Variable> result = Lists.newArrayList(drivers);
    Collections.sort(result);
    return Collections.unmodifiableList(result);
  }

  private void checkIntegrity() {
    for (Variable dependency : snapshots) {
      if (reads.contains(dependency) || writes.contains(dependency)) {
        throw new IllegalStateException(); // TODO message
      }
    }
    for (Variable dependency : reads) {
      if (writes.contains(dependency)) {
        throw new IllegalStateException(); // TODO message
      }
    }
  }

  public List<Variable> snapshots() {
    return snapshots;
  }

  public List<Variable> reads() {
    return reads;
  }

  public List<Variable> writes() {
    return writes;
  }

  public static class Builder {

    private List<Variable> snapshots = Collections.emptyList();
    private List<Variable> reads = Collections.emptyList();
    private List<Variable> writes = Collections.emptyList();

    public Builder snapshots(Variable... dependencies) {
      return snapshots(Arrays.asList(dependencies));
    }

    public Builder snapshots(Variable[]... dependencies) {
      return snapshots(concatenate(dependencies));
    }

    public Builder snapshots(List<Variable> dependencies) {
      this.snapshots = Lists.newArrayList(dependencies);
      return this;
    }

    public Builder reads(Variable... dependencies) {
      return reads(Arrays.asList(dependencies));
    }

    public Builder reads(Variable[]... dependencies) {
      return reads(concatenate(dependencies));
    }

    public Builder reads(List<Variable> dependencies) {
      this.reads = Lists.newArrayList(dependencies);
      return this;
    }

    public Builder writes(Variable... dependencies) {
      return writes(Arrays.asList(dependencies));
    }

    public Builder writes(Variable[]... dependencies) {
      return writes(concatenate(dependencies));
    }

    public Builder writes(List<Variable> drivers) {
      this.writes = Lists.newArrayList(drivers);
      return this;
    }

    private List<Variable> concatenate(Variable[]... lists) {
      List<Variable> result = Lists.newArrayList();
      for (Variable[] list : lists) {
        result.addAll(Arrays.asList(list));
      }
      return result;
    }

    public BaseDependency build() {
      return new BaseDependency(snapshots, reads, writes);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
