package com.liveramp.megadesk.recipes.actor;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.core.transaction.Transaction;

public abstract class Actor<State, Message> {

  private final ChannelImpl<Message> mailbox;
  private final TransactionExecutor executor;
  private final List<Transaction<Void>> sendOffs = Lists.newArrayList();
  private final Variable<State> state;
  private final ActorId actorId;


  protected Actor(Channel<Message> mailbox, TransactionExecutor executor, Variable<State> state, ActorId actorId) {
    this.state = state;
    this.actorId = actorId;
    this.mailbox = (ChannelImpl<Message>)mailbox;
    this.executor = executor;
  }

  protected Actor(String uniqueName, State state) {
    this.executor = new BaseTransactionExecutor();
    this.mailbox = new ChannelImpl<Message>(new InMemoryLocal<ImmutableList<Message>>(ImmutableList.<Message>of()));
    this.state = new InMemoryLocal<State>(state);
    this.actorId = new ActorId(uniqueName);
  }

  protected abstract State act(State state, Message m);


  public Transaction<Void> execute() {
    Message message = recv();
    State state = this.state.driver().persistence().read();
    State newState = act(state, message);
    final Transaction<Void> ack = mailbox.ack();
    List<Transaction> all = Lists.newArrayList();
    all.add(new UpdateState(newState));
    all.addAll(sendOffs);
    all.add(ack);
    Transaction<Void> transaction = new MergedTransaction(all);
    sendOffs.clear();
    return transaction;
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


  public RawAddress<Message> rawAddress() {
    return mailbox.getAddress();
  }

  public void spawn(ExecutorService service) {
    final Actor thisActor = this;
    final BaseTransactionExecutor exec = new BaseTransactionExecutor();
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

  public ActorId getActorId() {
    return actorId;
  }

  private class MergedTransaction implements Transaction<Void> {
    private final Dependency dependency;
    private List<Transaction> transactions;

    public MergedTransaction(List<Transaction> transactions) {
      this.transactions = Lists.newArrayList(transactions);
      List<Dependency> dependencies = Lists.newArrayList();
      for (Transaction transaction : this.transactions) {
        dependencies.add(transaction.dependency());
      }
      this.dependency = BaseDependency.merge(dependencies.toArray(new Dependency[transactions.size()]));
    }

    @Override
    public Dependency dependency() {
      return dependency;
    }

    @Override
    public Void run(Context context) throws Exception {
      for (Transaction<Void> transaction : transactions) {
        transaction.run(context);
      }
      return null;
    }
  }

  private class UpdateState implements Transaction<Void> {

    private State newState;

    private UpdateState(State newState) {
      this.newState = newState;
    }


    @Override
    public Dependency dependency() {
      return BaseDependency.builder().writes(state).build();
    }

    @Override
    public Void run(Context context) throws Exception {
      context.write(state, newState);
      return null;
    }
  }

  public Address address() {
    return this.mailbox.port();
  }
}
