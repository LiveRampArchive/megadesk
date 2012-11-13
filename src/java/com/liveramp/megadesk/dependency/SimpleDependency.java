package com.liveramp.megadesk.dependency;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.step.Step;

public abstract class SimpleDependency<T> implements Dependency<Object, T> {

  @Override
  public boolean check(Step<Object, ?> step, Resource<T> resource, DependencyWatcher watcher) throws Exception {
    return check(resource.read(watcher));
  }

  public abstract boolean check(T resourceData) throws Exception;
}
