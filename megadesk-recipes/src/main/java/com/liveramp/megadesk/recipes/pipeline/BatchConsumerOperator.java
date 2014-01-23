package com.liveramp.megadesk.recipes.pipeline;


import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.batch.Batch;
import com.liveramp.megadesk.recipes.gear.Outcome;

public class BatchConsumerOperator extends Operator {

  public BatchConsumerOperator(Batch batch, BaseDependency<Driver> dependency, Pipeline pipeline) {
    super(dependency, pipeline);
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {
      try {
        return execute(context);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      return check;
    }
  }

  @Override
  public Outcome execute(Context context) throws Exception {
    return null;
  }
}
