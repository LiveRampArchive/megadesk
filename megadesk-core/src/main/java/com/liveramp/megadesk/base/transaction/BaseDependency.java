package com.liveramp.megadesk.base.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.DependencyType;
import com.liveramp.megadesk.core.transaction.VariableDependency;

public class BaseDependency implements Dependency {

  private final List<VariableDependency> all;
  private final List<Variable> reads;
  private final List<Variable> writes;

  public BaseDependency(Collection<Variable> reads,
                        Collection<Variable> writes) {
    this.reads = prepareList(reads);
    this.writes = prepareList(writes);
    this.all = prepareAll();
    checkIntegrity();
  }

  private List<Variable> prepareList(Collection<Variable> dependencies) {
    // Deep copy, sort, and make unmodifiable
    List<Variable> result = Lists.newArrayList(dependencies);
    Collections.sort(result);
    return Collections.unmodifiableList(result);
  }

  private List<VariableDependency> prepareAll() {
    List<VariableDependency> result = Lists.newArrayList();
    for (Variable variable : reads) {
      result.add(BaseVariableDependency.build(variable, DependencyType.READ));
    }
    for (Variable variable : writes) {
      result.add(BaseVariableDependency.build(variable, DependencyType.WRITE));
    }
    Collections.sort(result);
    return Collections.unmodifiableList(result);
  }

  private void checkIntegrity() {
    checkDisjoint(reads, writes);
  }

  private void checkDisjoint(Collection<Variable> a, Collection<Variable> b) {
    if (!Collections.disjoint(a, b)) {
      throw new IllegalStateException(); // TODO message
    }
  }

  @Override
  public List<VariableDependency> all() {
    return all;
  }

  public List<Variable> reads() {
    return reads;
  }

  public List<Variable> writes() {
    return writes;
  }

  public static class Builder {

    private List<Variable> reads = Collections.emptyList();
    private List<Variable> writes = Collections.emptyList();

    public Builder all(VariableDependency... dependencies) {
      return all(Arrays.asList(dependencies));
    }

    public Builder all(VariableDependency[]... dependencies) {
      return all(concatenate(dependencies));
    }

    public Builder all(List<VariableDependency> dependencies) {
      reads = Lists.newArrayList();
      writes = Lists.newArrayList();
      for (VariableDependency dependency : dependencies) {
        switch (dependency.type()) {
          case READ:
            reads.add(dependency.variable());
            break;
          case WRITE:
            writes.add(dependency.variable());
            break;
          default:
            throw new IllegalStateException();
        }
      }
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

    public Builder writes(List<Variable> dependencies) {
      this.writes = Lists.newArrayList(dependencies);
      return this;
    }

    private List<Variable> concatenate(Variable[]... lists) {
      List<Variable> result = Lists.newArrayList();
      for (Variable[] list : lists) {
        result.addAll(Arrays.asList(list));
      }
      return result;
    }

    private List<VariableDependency> concatenate(VariableDependency[]... lists) {
      List<VariableDependency> result = Lists.newArrayList();
      for (VariableDependency[] list : lists) {
        result.addAll(Arrays.asList(list));
      }
      return result;
    }

    public BaseDependency build() {
      return new BaseDependency(reads, writes);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static BaseDependency merge(Dependency... dependencies) {
    Builder builder = BaseDependency.builder();
    List<VariableDependency> all = Lists.newArrayList();
    for (Dependency dependency : dependencies) {
      all.addAll(dependency.all());
    }
    return builder.all(all).build();
  }
}
