package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.state.EqualityStateChecker;
import com.liveramp.megadesk.state.StateCheck;

public class Read {

  private final Resource resource;
  private final StateCheck stateCheck;

  public Read(Resource resource, String state) {
    this.resource = resource;
    this.stateCheck = new EqualityStateChecker(state);
  }

  public Read(Resource resource, StateCheck stateCheck) {
    this.resource = resource;
    this.stateCheck = stateCheck;
  }

  public Resource getResource() {
    return resource;
  }

  public StateCheck getStateCheck() {
    return stateCheck;
  }
}
