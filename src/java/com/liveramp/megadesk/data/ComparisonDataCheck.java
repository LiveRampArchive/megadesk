package com.liveramp.megadesk.data;

import com.liveramp.megadesk.resource.Resource;

import java.util.ArrayList;
import java.util.List;

public class ComparisonDataCheck<T extends Comparable<T>> implements DataCheck<T> {

  private final T data;
  private final List<Integer> acceptableComparisonResults;

  public ComparisonDataCheck(T data, int... acceptableComparisonResults) {
    this.data = data;
    this.acceptableComparisonResults = new ArrayList<Integer>(acceptableComparisonResults.length);
    for (int acceptableComparisonResult : acceptableComparisonResults) {
      this.acceptableComparisonResults.add(sign(acceptableComparisonResult));
    }
  }

  @Override
  public boolean check(Resource<T> resource) throws Exception {
    T currentData = resource.getData();
    if (data == null || currentData == null) {
      return false;
    } else {
      return acceptableComparisonResults.contains(sign(currentData.compareTo(data)));
    }
  }

  @Override
  public String toString() {
    return "[ComparisonDataCheck: " + acceptableComparisonResults
        + " when compared to " + data + "]";
  }

  private int sign(int result) {
    return result == 0 ? 0 : result > 0 ? 1 : -1;
  }
}
