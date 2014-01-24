package com.liveramp.megadesk.recipes.pipeline.lib;

import com.liveramp.megadesk.base.state.InMemoryReadWriteLock;
import com.liveramp.megadesk.base.state.InMemoryReference;
import com.liveramp.megadesk.recipes.pipeline.BaseTimestampedDriver;
import com.liveramp.megadesk.recipes.pipeline.TimestampedDriver;

public class InMemoryTimestampedDriver<VALUE> extends BaseTimestampedDriver<VALUE> implements TimestampedDriver<VALUE> {

  public InMemoryTimestampedDriver(VALUE value) {
    super(new InMemoryReference<VALUE>(), new InMemoryTimestampedPersistence<VALUE>(value),
        new InMemoryReadWriteLock(), new InMemoryReadWriteLock());
  }
}
