package com.liveramp.megadesk.resource;

public class Dependency {

  private final Resource resource;
  private final String state;

  public Dependency(Resource resource, String state) {
    this.state = state;
    this.resource = resource;
  }

  public Resource getResource() {
    return resource;
  }

  public String getState() {
    return state;
  }
}
