package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.state.check.StateCheck;

public class Read {

  private final Resource resource;
  private final StateCheck stateCheck;

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
