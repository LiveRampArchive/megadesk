package com.liveramp.megadesk.recipes.pipeline;

import java.util.List;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryDriver;
import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.base.state.Param;
import com.liveramp.megadesk.base.transaction.Bind;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.BaseGearIteration;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.iteration.BaseIterationExecutor;
import com.liveramp.megadesk.recipes.iteration.IterationExecutor;
import com.liveramp.megadesk.test.BaseTestCase;

public class TestPipeline extends BaseTestCase {

  private final IterationExecutor iterationExecutor = new BaseIterationExecutor();

  @Test
  public void testTimeBasedPipeline() throws InterruptedException {

    Variable<TimestampedInteger> step1 = getTimestampedInt(0);
    Variable<TimestampedInteger> step2 = getTimestampedInt(0);
    Variable<TimestampedInteger> step3 = getTimestampedInt(0);
    Variable<TimestampedInteger> step4 = getTimestampedInt(0);

    step1.driver().persistence().write(new TimestampedInteger(1));

    Pipeline pipeline = new Pipeline() {
      @Override
      public boolean shouldShutdown() {
        return false;
      }
    };

    AddOne addOne = new AddOne(pipeline);

    iterationExecutor.execute(new BaseGearIteration(addOne, new Bind(step1, step2)));
    iterationExecutor.execute(new BaseGearIteration(addOne, new Bind(step2, step3)));
    iterationExecutor.execute(new BaseGearIteration(addOne, new Bind(step3, step4)));

    waitUntilValueEqual(step4, 4, 1000);

    Assert.assertEquals(Integer.valueOf(1), step1.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(2), step2.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(3), step3.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(4), step4.driver().persistence().read().getInteger());

    step1.driver().persistence().write(new TimestampedInteger(10));

    waitUntilValueEqual(step4, 13, 1000);

    Assert.assertEquals(Integer.valueOf(10), step1.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(11), step2.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(12), step3.driver().persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(13), step4.driver().persistence().read().getInteger());
  }

  private void waitUntilValueEqual(Variable variable, Object value, int timeout) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (!variable.driver().persistence().read().equals(value) && System.currentTimeMillis() - start < timeout) {
      Thread.sleep(10);
    }
  }

  private Local<TimestampedInteger> getTimestampedInt(int i) {
    return new Local<TimestampedInteger>(new InMemoryDriver<TimestampedInteger>(new TimestampedInteger(i)));
  }

  private static class AddOne extends TimeBasedOperator {

    private final Variable<TimestampedInteger> input;
    private final Variable<TimestampedInteger> output;

    protected AddOne(Pipeline pipeline) {
      this(new Param<TimestampedInteger>(0), new Param<TimestampedInteger>(1), pipeline);
    }

    protected AddOne(Variable<TimestampedInteger> input, Variable<TimestampedInteger> output, Pipeline pipeline) {
      super((List)Lists.newArrayList(input), (List)Lists.newArrayList(output), pipeline);
      this.input = input;
      this.output = output;
    }

    @Override
    public Outcome execute(Context context) throws Exception {
      Integer integer = context.read(input).getInteger();
      context.write(output, new TimestampedInteger(integer + 1));
      return Outcome.SUCCESS;
    }
  }

  private static class TimestampedInteger implements TimestampedValue {
    private final Integer integer;
    private final Long timestamp;

    private TimestampedInteger(Integer integer) {
      this.integer = integer;
      timestamp = System.currentTimeMillis();
    }

    @Override
    public long timestamp() {
      return timestamp;
    }

    private Integer getInteger() {
      return integer;
    }
  }
}
