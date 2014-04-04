package com.liveramp.megadesk.recipes.actor;

import com.liveramp.megadesk.core.transaction.Transaction;

public interface Channel<Message> {


  public Transaction<Void> send(Message message);

  public Transaction<Message> recv();

  public Transaction<Void> ack();

  public RawAddress<Message> getAddress();

}
