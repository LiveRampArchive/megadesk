package com.liveramp.megadesk.recipes.actor;

import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;

public class Address<Message> {

  private final RawAddress<Message> add;
  private final BaseTransactionExecutor exec;

  public Address(RawAddress<Message> add, BaseTransactionExecutor exec) {
    this.add = add;
    this.exec = exec;
  }

  public void send(Message m) {
    try {
      exec.execute(add.send(m));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
