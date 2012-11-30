package com.liveramp.megadesk.dependency;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.step.Step;

import java.util.Collections;
import java.util.Set;

public abstract class SimpleDependency<RESOURCE> implements Dependency {

  private final Resource<RESOURCE> resource;

  public SimpleDependency(Resource<RESOURCE> resource) {
    this.resource = resource;
  }

  @Override
  public Set<Resource> getResources() {
    return Collections.singleton((Resource) resource);
  }

  @Override
  public boolean check(Step step, DependencyWatcher watcher) throws Exception {
    return check(resource.read(watcher));
  }

  public abstract boolean check(RESOURCE resourceData) throws Exception;
}
