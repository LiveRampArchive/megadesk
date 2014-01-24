package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

class Append<V> implements Transaction<Void> {

  private final Dependency dependency;
  private final Reference<ImmutableList> reference;
  private final V value;

  Append(Driver<ImmutableList> driver, V value) {
    this.value = value;
    this.reference = driver.reference();
    this.dependency = BaseDependency.builder().writes(driver).build();
  }

  @Override
  public Dependency dependency() {
    return dependency;
  }

  @Override
  public Void run(Context context) throws Exception {
    ImmutableList originalValue = context.read(reference);
    ImmutableList newValue = ImmutableList.builder().addAll(originalValue).add(value).build();
    context.write(reference, newValue);
    return null;
  }
}
