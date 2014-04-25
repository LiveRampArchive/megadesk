package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.state.BaseVariable;
import com.liveramp.megadesk.base.state.Name;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;

public class QueueExecutable<VALUE> extends BaseQueueExecutable<VALUE, VALUE> implements MegadeskPersistentQueue<VALUE, VALUE> {

  public QueueExecutable(Queue<VALUE> queue, TransactionExecutor executor) {
    super(queue, executor);
  }

  public static <VALUE> QueueExecutable<VALUE> getQueueByName(String name, DriverFactory factory, BaseTransactionExecutor executor) {
    return new QueueExecutable<VALUE>(
        new Queue<VALUE>(
            new BaseVariable<ImmutableList>(new Name<ImmutableList>(name + "input"), factory.<ImmutableList>get(name + "-input", ImmutableList.of())),
            new BaseVariable<ImmutableList>(new Name<ImmutableList>(name + "output"), factory.<ImmutableList>get(name + "-output", ImmutableList.of())),
            new BaseVariable<Boolean>(new Name<Boolean>(name + "frozen"), (factory.get(name + "-frozen", false)))),
        new BaseTransactionExecutor());
  }

  public Queue<VALUE> getQueue() {
    return (Queue<VALUE>) super.getQueue();
  }
}
