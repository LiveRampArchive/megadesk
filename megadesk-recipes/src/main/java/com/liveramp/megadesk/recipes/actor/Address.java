package com.liveramp.megadesk.recipes.actor;

import com.liveramp.megadesk.base.transaction.BaseExecutor;

public class Address<Message> {

  private final RawAddress<Message> add;
  private final BaseExecutor exec;

  public Address(RawAddress<Message> add, BaseExecutor exec) {
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
