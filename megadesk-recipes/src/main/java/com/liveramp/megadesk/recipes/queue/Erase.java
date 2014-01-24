package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

class Erase implements Transaction<Void> {

  private final Dependency dependency;
  private final Reference<ImmutableList> listReference;
  private final Reference<Boolean> frozen;

  Erase(Driver<ImmutableList> listDriver, Driver<Boolean> frozen) {
    this.frozen = frozen.reference();
    this.listReference = listDriver.reference();
    this.dependency = BaseDependency.builder().writes(listDriver, frozen).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    context.write(listReference, ImmutableList.of());
    context.write(frozen, false);
    return null;
  }
}
