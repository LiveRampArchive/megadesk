package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Queue<VALUE> extends BaseQueue<VALUE> {

  public Queue(Driver<ImmutableList> input, Driver<ImmutableList> output, Driver<Boolean> frozen) {
    super(input, output, frozen);
  }

  @Override
  protected Transaction getPopTransaction() {
    return new PopOne(this.getOutput(), this.getFrozen());
  }
}
