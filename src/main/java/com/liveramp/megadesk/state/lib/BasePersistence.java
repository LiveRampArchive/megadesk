package com.liveramp.megadesk.state.lib;

import com.liveramp.megadesk.state.Persistence;

public abstract class BasePersistence<VALUE> implements Persistence<VALUE> {
  @Override
  public VALUE get() {
    return read().get();
  }
}
