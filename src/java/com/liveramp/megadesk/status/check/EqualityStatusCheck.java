package com.liveramp.megadesk.status.check;

import com.liveramp.megadesk.device.Device;

public class EqualityStatusCheck<T> implements StatusCheck<T> {

  private final T status;

  public EqualityStatusCheck(T status) {
    this.status = status;
  }

  @Override
  public boolean check(Device<T> device) throws Exception {
    T currentStatus = device.getStatus();
    return (status == null && currentStatus == null)
        || (status != null && status.equals(currentStatus));
  }

  @Override
  public String toString() {
    return "[EqualityStatusCheck: '" + status + "']";
  }
}
