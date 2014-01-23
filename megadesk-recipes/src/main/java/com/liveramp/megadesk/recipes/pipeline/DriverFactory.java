package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.state.Driver;

public interface DriverFactory {

  public <T> Driver<T> get(String referenceName);
}
