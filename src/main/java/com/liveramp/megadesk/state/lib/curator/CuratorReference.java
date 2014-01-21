package com.liveramp.megadesk.state.lib.curator;

import com.liveramp.megadesk.state.Reference;

public class CuratorReference<VALUE> implements Reference<VALUE> {

  private final String name;

  public CuratorReference(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int compareTo(Reference<VALUE> valueReference) {
    return this.name().compareTo(valueReference.name());
  }
}
