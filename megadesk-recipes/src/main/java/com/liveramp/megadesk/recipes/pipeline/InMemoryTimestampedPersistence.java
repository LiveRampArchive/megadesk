package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.base.state.InMemoryPersistence;

public class InMemoryTimestampedPersistence<VALUE> extends InMemoryPersistence<VALUE> implements TimestampedPersistence<VALUE> {

  private Long modifiedTime = System.currentTimeMillis();

  public InMemoryTimestampedPersistence(VALUE value) {
    super(value);
  }

  @Override
  public void write(VALUE value) {
    super.write(value);
    this.modifiedTime = System.currentTimeMillis();
  }

  @Override
  public long modified() {
    return modifiedTime;
  }
}
