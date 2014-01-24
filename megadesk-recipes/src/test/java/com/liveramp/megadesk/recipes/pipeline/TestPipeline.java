package com.liveramp.megadesk.recipes.pipeline;

import com.google.common.collect.Lists;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.gear.worker.NaiveWorker;
import com.liveramp.megadesk.recipes.pipeline.lib.InMemoryTimestampedDriver;
import com.liveramp.megadesk.test.BaseTestCase;
import junit.framework.Assert;
import org.junit.Test;

public class TestPipeline extends BaseTestCase {

  @Test
  public void testTimeBasedPipeline() throws InterruptedException {

    TimestampedDriver<Integer> step1 = new InMemoryTimestampedDriver<Integer>(0);
    TimestampedDriver<Integer> step2 = new InMemoryTimestampedDriver<Integer>(0);
    TimestampedDriver<Integer> step3 = new InMemoryTimestampedDriver<Integer>(0);
    TimestampedDriver<Integer> step4 = new InMemoryTimestampedDriver<Integer>(0);

    step1.persistence().write(1);

    Pipeline pipeline = new Pipeline() {
      @Override
      public boolean shouldShutdown() {
        return false;
      }
    };

    AddOne addOne1 = new AddOne(step1, step2, pipeline);
    AddOne addOne2 = new AddOne(step2, step3, pipeline);
    AddOne addOne3 = new AddOne(step3, step4, pipeline);

    new NaiveWorker().complete(addOne1, addOne2, addOne3);

    Assert.assertEquals(Integer.valueOf(1), step1.persistence().read());
    Assert.assertEquals(Integer.valueOf(2), step2.persistence().read());
    Assert.assertEquals(Integer.valueOf(3), step3.persistence().read());
    Assert.assertEquals(Integer.valueOf(4), step4.persistence().read());
  }

  private static class AddOne extends TimeBasedOperator {

    private final Reference<Integer> input;
    private final Reference<Integer> output;

    protected AddOne(TimestampedDriver<Integer> input, TimestampedDriver<Integer> output, Pipeline pipeline) {
      super(Lists.<TimestampedDriver>newArrayList(input), Lists.<TimestampedDriver>newArrayList(output), pipeline);
      this.input = input.reference();
      this.output = output.reference();
    }

    @Override
    public Outcome execute(Context context) throws Exception {
      context.write(output, context.read(input) + 1);
      System.out.println("Added one!");
      return Outcome.ABANDON;
    }
  }
}
