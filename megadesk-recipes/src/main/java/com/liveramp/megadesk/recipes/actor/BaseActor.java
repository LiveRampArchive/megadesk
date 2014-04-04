package com.liveramp.megadesk.recipes.actor;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.core.transaction.Transaction;

public abstract class BaseActor<Message> implements Actor<Message> {

  private final ChannelImpl<Message> mailbox;
  private final Executor executor;
  private final List<Transaction<Void>> sendOffs = Lists.newArrayList();


  protected BaseActor(Channel<Message> mailbox, Executor executor) {
    this.mailbox = (ChannelImpl<Message>)mailbox;
    this.executor = executor;
  }

  protected BaseActor() {
    this.executor = new BaseExecutor();
    this.mailbox = new ChannelImpl<Message>(new InMemoryLocal<ImmutableList<Message>>(ImmutableList.<Message>of()));
  }

  protected abstract void act(Message m);


  public Transaction<Void> execute() {
    Message message = recv();
    act(message);
    Transaction<Void> ack = ack();
    sendOffs.clear();
    return ack;
  }


  protected Message recv() {
    try {
      Message message = null;
      while (message == null) {
        message = executor.execute(mailbox.recv());
        Thread.yield();
      }
      return message;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> void send(RawAddress<T> rawAddress, T message) {
    Transaction<Void> sendMessageToChannel = rawAddress.send(message);
    sendOffs.add(sendMessageToChannel);
  }

  protected Transaction<Void> ack() {

    final Transaction<Void> ack = mailbox.ack();
    Transaction<Void> transaction = new MergedTransaction(sendOffs, ack);
    return transaction;
  }

  public RawAddress<Message> rawAddress() {
    return mailbox.getAddress();
  }

  public void spawn(ExecutorService service) {
    final Actor thisActor = this;
    final BaseExecutor exec = new BaseExecutor();
    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        while (true) {
          Transaction finalizingTransaction = thisActor.execute();
          try {
            exec.execute(finalizingTransaction);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    service.submit(runnable);
  }

  private class MergedTransaction implements Transaction<Void> {
    private final Dependency dependency;
    private List<Transaction<Void>> sendOffs = Lists.newArrayList();
    private final Transaction<Void> ack;

    public MergedTransaction(List<Transaction<Void>> sendOffs, Transaction<Void> ack) {
      for (Transaction<Void> sendOff : sendOffs) {
        this.sendOffs.add(sendOff);
      }
      this.ack = ack;
      this.dependency = getDependency();
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    private Dependency getDependency() {
      List<Dependency> dependencies = Lists.newArrayList();
      for (Transaction<Void> sendOff : sendOffs) {
        dependencies.add(sendOff.dependency());
      }
      dependencies.add(ack.dependency());
      return BaseDependency.merge(dependencies.toArray(new Dependency[sendOffs.size() + 1]));
    }

    @Override
    public Void run(Context context) throws Exception {
      for (Transaction<Void> sendOff : sendOffs) {
        sendOff.run(context);
      }
      ack.run(context);
      return null;
    }
  }

  public Address address() {
    return this.mailbox.port();
  }
}
