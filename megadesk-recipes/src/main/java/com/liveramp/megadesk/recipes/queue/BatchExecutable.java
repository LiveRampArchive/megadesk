package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;

public class BatchExecutable<VALUE> extends BaseQueueExecutable<VALUE, ImmutableList<VALUE>> implements MegadeskPersistentBatch<VALUE> {

  public BatchExecutable(Batch<VALUE> batch, Executor executor) {
    super(batch, executor);
  }

  public static <VALUE> BatchExecutable<VALUE> getBatchByName(String name, DriverFactory factory, BaseExecutor executor) {
    return new BatchExecutable<VALUE>(
        new Batch<VALUE>(
            new Local<ImmutableList>(factory.<ImmutableList>get(name + "-input", ImmutableList.of())),
            new Local<ImmutableList>(factory.<ImmutableList>get(name + "-output", ImmutableList.of())),
            new Local<Boolean>(factory.get(name + "-frozen", false))),
        new BaseExecutor());
  }
}
