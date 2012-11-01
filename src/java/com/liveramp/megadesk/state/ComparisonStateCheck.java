package com.liveramp.megadesk.state;

import com.liveramp.megadesk.resource.Resource;

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
  public boolean check(Resource<T> resource) throws Exception {
    T currentState = resource.getState();
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
