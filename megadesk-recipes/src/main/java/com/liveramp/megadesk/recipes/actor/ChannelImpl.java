package com.liveramp.megadesk.recipes.actor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.recipes.queue.Append;

public class ChannelImpl<Message> implements Channel<Message>, RawAddress<Message> {

  private final Variable<ImmutableList<Message>> queue;

  public ChannelImpl(Variable<ImmutableList<Message>> queue) {
    this.queue = queue;
  }

  @Override
  public Transaction<Void> send(Message message) {
    Append append = new Append(queue, Lists.newArrayList(message));
    return append;
  }

  @Override
  public Transaction<Message> recv() {
    return new Read(queue);
  }

  @Override
  public Transaction<Void> ack() {
    return new Ack(queue);
  }

  @Override
  public RawAddress<Message> getAddress() {
    return this;
  }

  private class Read implements Transaction<Message> {

    private final Variable<ImmutableList<Message>> list;

    private Read(Variable<ImmutableList<Message>> list) {
      this.list = list;
    }

    @Override
    public Dependency dependency() {
      return BaseDependency.builder().reads(list).build();
    }

    @Override
    public Message run(Context context) throws Exception {
      ImmutableList<Message> messageQueue = context.read(list.reference());
      if (messageQueue.isEmpty()) {
        return null;
      }
      return messageQueue.get(0);
    }
  }


  private class Ack implements Transaction<Void> {
    private Variable<ImmutableList<Message>> queue;

    public Ack(Variable<ImmutableList<Message>> queue) {
      this.queue = queue;
    }

    @Override
    public Dependency dependency() {
      return BaseDependency.builder().writes(queue).build();
    }

    @Override
    public Void run(Context context) throws Exception {
      ImmutableList<Message> messageQueue = context.read(queue.reference());
      ImmutableList<Message> messages = messageQueue.subList(1, messageQueue.size());
      context.write(queue.reference(), messages);
      return null;
    }
  }

  public Address port() {
    return new Address(this, new BaseExecutor());
  }
}
