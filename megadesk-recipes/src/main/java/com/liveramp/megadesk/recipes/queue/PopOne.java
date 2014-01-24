package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public class PopOne implements Transaction<Void> {

  private final Dependency dependency;
  private final Variable<ImmutableList> list;
  private final Variable<Boolean> frozen;

  PopOne(Variable<ImmutableList> list, Variable<Boolean> frozen) {
    this.frozen = frozen;
    this.list = list;
    this.dependency = BaseDependency.builder().writes(this.list, this.frozen).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList list = context.read(this.list);
    if (!list.isEmpty()) {
      ImmutableList newList = list.subList(1, list.size());
      context.write(this.list, newList);
    }
    if (context.read(this.list).isEmpty()) {
      context.write(this.frozen, false);
    }
    return null;
  }
}
