package com.liveramp.megadesk.state;

public class EqualityStateChecker implements StateCheck {

  private final String state;

  public EqualityStateChecker(String state) {
    this.state = state;
  }

  @Override
  public boolean check(String state) {
    return (this.state == null && state == null)
        || (this.state != null && this.state.equals(state));
  }

  @Override
  public String toString() {
    return "[EqualityStateCheck: '" + state + "']";
  }
}
