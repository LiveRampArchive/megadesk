package com.liveramp.megadesk.recipes.agent;

import java.util.concurrent.Callable;

public class Execution {

  private final Condition condition;
  private final Callable<Callable> function;

  public Execution(Condition condition, Callable<Callable> function) {
    this.condition = condition;
    this.function = function;
  }
}
