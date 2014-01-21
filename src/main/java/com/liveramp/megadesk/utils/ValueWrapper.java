package com.liveramp.megadesk.utils;

import com.liveramp.megadesk.state.Value;

public interface ValueWrapper {

  public <T> Value<T> wrap(T value);
}
