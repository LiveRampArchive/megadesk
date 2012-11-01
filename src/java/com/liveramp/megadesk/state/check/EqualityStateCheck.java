package com.liveramp.megadesk.state.check;

import com.liveramp.megadesk.resource.Resource;

public class EqualityStateCheck<T> implements StateCheck<T> {

  private final T state;

  public EqualityStateCheck(T state) {
    this.state = state;
  }

  @Override
  public boolean check(Resource<T> resource) throws Exception {
    T currentState = resource.getState();
    return (state == null && currentState == null)
        || (state != null && state.equals(currentState));
  }

  @Override
  public String toString() {
    return "[EqualityStateCheck: '" + state + "']";
  }
}
