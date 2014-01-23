package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.state.Driver;

public interface TimestampedDriver<T> extends Driver<T> {

  public long modified();
}
