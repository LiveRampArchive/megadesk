package com.liveramp.megadesk.recipes.queue;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

class Append<V> implements Transaction<Void> {

  private final Dependency dependency;
  private final Variable<ImmutableList> list;
  private final List<V> values;

  Append(Variable<ImmutableList> driver, List<V> values) {
    this.values = values;
    this.list = driver;
    this.dependency = BaseDependency.builder().writes(list).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList originalValue = context.read(list);
    ImmutableList newValue = ImmutableList.builder().addAll(originalValue).addAll(values).build();
    context.write(list, newValue);
    return null;
  }
}
