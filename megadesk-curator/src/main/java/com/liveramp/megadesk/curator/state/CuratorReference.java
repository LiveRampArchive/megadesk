package com.liveramp.megadesk.curator.state;

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

  @Override
  public boolean equals(Object o) {
    if (o instanceof Reference) {
      return this.name().equals(((Reference) o).name());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return name().hashCode();
  }
}
