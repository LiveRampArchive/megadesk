package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Queue<VALUE> extends BaseQueue<VALUE, VALUE> {

  public Queue(Variable<ImmutableList> input,Variable<ImmutableList> output, Variable<Boolean> frozen) {
    super(input, output, frozen);
  }

  @Override
  protected VALUE internalRead(ImmutableList<VALUE> transfer) {
    if (transfer.isEmpty()) {
      return null;
    } else {
      return transfer.get(0);
    }
  }

  @Override
  protected Transaction getPopTransaction() {
    return new PopOne(this.getOutput(), this.getFrozen());
  }
}
