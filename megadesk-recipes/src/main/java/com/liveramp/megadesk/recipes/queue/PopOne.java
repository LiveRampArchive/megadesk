package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public class PopOne implements Transaction<Void> {

  private final Dependency dependency;
  private final Variable<ImmutableList> list;
  private final Variable<Boolean> frozen;

  PopOne(Driver<ImmutableList> listDriver, Driver<Boolean> frozenDriver) {
    this.frozen = new Local<Boolean>(frozenDriver);
    this.list = new Local<ImmutableList>(listDriver);
    this.dependency = BaseDependency.builder().writes(list, frozen).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList list = context.read(this.list);
    ImmutableList newList = list.subList(1, list.size());
    context.write(this.list, newList);
    if (newList.isEmpty()) {
      context.write(frozen, false);
    }
    return null;
  }
}
