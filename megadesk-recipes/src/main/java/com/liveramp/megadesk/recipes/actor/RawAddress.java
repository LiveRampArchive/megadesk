package com.liveramp.megadesk.recipes.actor;

import com.liveramp.megadesk.core.transaction.Transaction;

public interface RawAddress<Message> {

  public Transaction<Void> send(Message message);
}
