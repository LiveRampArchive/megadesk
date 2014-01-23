package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.recipes.Batch;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.Context;

public class BatchConsumerOperator extends Operator {

  public BatchConsumerOperator(Batch batch, BaseDependency<Driver> dependency, Pipeline pipeline) {
    super(dependency, pipeline);
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {

    } else {
      return check;
    }
  }

  @Override
  public Outcome execute(Context context) throws Exception {
    return null;
  }
}
