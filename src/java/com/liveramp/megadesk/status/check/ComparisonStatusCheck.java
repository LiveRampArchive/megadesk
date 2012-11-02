package com.liveramp.megadesk.status.check;

import com.liveramp.megadesk.device.Device;

import java.util.ArrayList;
import java.util.List;

public class ComparisonStatusCheck<T extends Comparable<T>> implements StatusCheck<T> {

  private final T status;
  private final List<Integer> acceptableComparisonResults;

  public ComparisonStatusCheck(T status, int... acceptableComparisonResults) {
    this.status = status;
    this.acceptableComparisonResults = new ArrayList<Integer>(acceptableComparisonResults.length);
    for (int acceptableComparisonResult : acceptableComparisonResults) {
      this.acceptableComparisonResults.add(sign(acceptableComparisonResult));
    }
  }

  @Override
  public boolean check(Device<T> device) throws Exception {
    T currentStatus = device.getStatus();
    if (status == null || currentStatus == null) {
      return false;
    } else {
      return acceptableComparisonResults.contains(sign(currentStatus.compareTo(status)));
    }
  }

  @Override
  public String toString() {
    return "[ComparisonStatusCheck: " + acceptableComparisonResults
        + " when compared to " + status + "]";
  }

  private int sign(int result) {
    return result == 0 ? 0 : result > 0 ? 1 : -1;
  }
}
