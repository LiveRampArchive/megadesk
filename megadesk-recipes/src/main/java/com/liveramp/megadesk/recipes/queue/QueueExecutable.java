package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.state.BaseVariable;
import com.liveramp.megadesk.base.state.Name;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;

public class QueueExecutable<VALUE> extends BaseQueueExecutable<VALUE, VALUE> implements MegadeskPersistentQueue<VALUE, VALUE> {

  public QueueExecutable(Queue<VALUE> queue, Executor executor) {
    super(queue, executor);
  }

  public static <VALUE> QueueExecutable<VALUE> getQueueByName(String name, DriverFactory factory, BaseExecutor executor) {
    return new QueueExecutable<VALUE>(
        new Queue<VALUE>(
            new BaseVariable<ImmutableList>(new Name<ImmutableList>(name + "input"), factory.<ImmutableList>get(name + "-input", ImmutableList.of())),
            new BaseVariable<ImmutableList>(new Name<ImmutableList>(name + "output"), factory.<ImmutableList>get(name + "-output", ImmutableList.of())),
            new BaseVariable<Boolean>(new Name<Boolean>(name + "frozen"), (factory.get(name + "-frozen", false)))),
        new BaseExecutor());
  }

  public Queue<VALUE> getQueue() {
    return (Queue<VALUE>) super.getQueue();
  }
}
