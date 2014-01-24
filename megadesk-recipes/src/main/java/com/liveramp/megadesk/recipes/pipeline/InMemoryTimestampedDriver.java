package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.base.state.InMemoryReadWriteLock;
import com.liveramp.megadesk.base.state.InMemoryReference;

public class InMemoryTimestampedDriver<VALUE> extends TimestampedDriverImpl<VALUE> implements TimestampedDriver<VALUE> {

  public InMemoryTimestampedDriver(VALUE value) {
    super(new InMemoryReference<VALUE>(), new InMemoryTimestampedPersistence<VALUE>(value),
        new InMemoryReadWriteLock(), new InMemoryReadWriteLock());
  }
}
