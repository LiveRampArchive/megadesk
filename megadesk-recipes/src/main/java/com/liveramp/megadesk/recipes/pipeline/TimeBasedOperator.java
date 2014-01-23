package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.Context;

import java.util.List;

public abstract class TimeBasedOperator extends Operator implements Gear {

  protected TimeBasedOperator(List<TimestampedDriver> reads, List<TimestampedDriver> writes, Pipeline pipeline) {
    super(BaseDependency.<Driver>builder().reads((List) reads).writes((List) writes).build(), pipeline);
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {
      long oldestRead = Long.MAX_VALUE;
      for (Driver driver : this.dependency().reads()) {
        if (driver instanceof TimestampedDriver) {
          oldestRead = Math.min(((TimestampedDriver) driver).modified(), oldestRead);
        }
      }
      long youngestWrite = 0;
      for (Driver driver : this.dependency().writes()) {
        if (driver instanceof TimestampedDriver) {
          youngestWrite = Math.max(((TimestampedDriver) driver).modified(), youngestWrite);
        }
      }
      if (oldestRead > youngestWrite) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    } else {
      return check;
    }
  }
}
