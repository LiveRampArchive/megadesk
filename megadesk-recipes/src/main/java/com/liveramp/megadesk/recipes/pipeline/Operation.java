package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.gear.ConditionalGear;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.Context;

import java.util.List;

public abstract class Operation extends ConditionalGear implements Gear {

  private final Operator operator;
  private final List<Operation> previousOperations;
  private final Driver<Boolean> checkpoint;

  protected Operation(
      Driver<Boolean> checkpoint,
      Operator operator,
      List<Operation> previousOperations) {
    super(BaseDependency.<Driver>builder()
        .reads(operator.dependency().reads())
        .reads((List) previousOperations)
        .writes(operator.dependency().writes())
        .writes(checkpoint)
        .build());

    this.operator = operator;
    this.previousOperations = previousOperations;
    this.checkpoint = checkpoint;
  }

  public Reference<Boolean> getCheckpoint() {
    return checkpoint.reference();
  }

  public List<Operation> getPreviousOperations() {
    return previousOperations;
  }

  @Override
  public Outcome check(Context context) {
    for (Operation operation : previousOperations) {
      if (!context.read(operation.getCheckpoint())) {
        return Outcome.STANDBY;
      }
    }
    return Outcome.SUCCESS;
  }

  @Override
  public Outcome execute(Context context) throws Exception {
    Outcome outcome = operator.run(context);
    if (outcome == Outcome.SUCCESS) {
      context.write(this.getCheckpoint(), true);
    }
    return outcome;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Operation) {
      return ((Operation) o).getCheckpoint().equals(this.getCheckpoint());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.getCheckpoint().hashCode();
  }
}
