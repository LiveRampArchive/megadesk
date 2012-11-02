package com.liveramp.megadesk.state.check;

import com.liveramp.megadesk.device.Device;

import java.util.ArrayList;
import java.util.List;

public class ComparisonStateCheck<T extends Comparable<T>> implements StateCheck<T> {

  private final T state;
  private final List<Integer> acceptableComparisonResults;

  public ComparisonStateCheck(T state, int... acceptableComparisonResults) {
    this.state = state;
    this.acceptableComparisonResults = new ArrayList<Integer>(acceptableComparisonResults.length);
    for (int acceptableComparisonResult : acceptableComparisonResults) {
      this.acceptableComparisonResults.add(sign(acceptableComparisonResult));
    }
  }

  @Override
  public boolean check(Device<T> device) throws Exception {
    T currentState = device.getState();
    if (state == null || currentState == null) {
      return false;
    } else {
      return acceptableComparisonResults.contains(sign(currentState.compareTo(state)));
    }
  }

  @Override
  public String toString() {
    return "[ComparisonStateCheck: " + acceptableComparisonResults
        + " when compared to " + state + "]";
  }

  private int sign(int result) {
    return result == 0 ? 0 : result > 0 ? 1 : -1;
  }
}
