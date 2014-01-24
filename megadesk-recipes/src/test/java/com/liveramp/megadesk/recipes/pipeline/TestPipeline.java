package com.liveramp.megadesk.recipes.pipeline;

import com.google.common.collect.Lists;
import com.liveramp.megadesk.base.state.InMemoryDriver;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Reference;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.gear.worker.NaiveWorker;
import com.liveramp.megadesk.test.BaseTestCase;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

public class TestPipeline extends BaseTestCase {

  @Test
  public void testTimeBasedPipeline() throws InterruptedException {

    Driver<TimestampedInteger> step1 = new InMemoryDriver<TimestampedInteger>(new TimestampedInteger(0));
    Driver<TimestampedInteger> step2 = new InMemoryDriver<TimestampedInteger>(new TimestampedInteger(0));
    Driver<TimestampedInteger> step3 = new InMemoryDriver<TimestampedInteger>(new TimestampedInteger(0));
    Driver<TimestampedInteger> step4 = new InMemoryDriver<TimestampedInteger>(new TimestampedInteger(0));

    step1.persistence().write(new TimestampedInteger(1));

    Pipeline pipeline = new Pipeline() {
      @Override
      public boolean shouldShutdown() {
        return false;
      }
    };

    AddOne addOne1 = new AddOne(step1, step2, pipeline, "one");
    AddOne addOne2 = new AddOne(step2, step3, pipeline, "two");
    AddOne addOne3 = new AddOne(step3, step4, pipeline, "three");

    NaiveWorker worker = new NaiveWorker();
    worker.run(addOne1, addOne2, addOne3);

    Thread.sleep(1000);

    Assert.assertEquals(Integer.valueOf(1), step1.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(2), step2.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(3), step3.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(4), step4.persistence().read().getInteger());

    step1.persistence().write(new TimestampedInteger(10));

    Thread.sleep(1000);

    Assert.assertEquals(Integer.valueOf(10), step1.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(11), step2.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(12), step3.persistence().read().getInteger());
    Assert.assertEquals(Integer.valueOf(13), step4.persistence().read().getInteger());

  }

  private static class AddOne extends TimeBasedOperator {

    private final Reference<TimestampedInteger> input;
    private final Reference<TimestampedInteger> output;
    private final String name;

    protected AddOne(Driver<TimestampedInteger> input, Driver<TimestampedInteger> output, Pipeline pipeline, String name) {
      super((List) Lists.newArrayList(input), (List) Lists.<Driver<TimestampedInteger>>newArrayList(output), pipeline);
      this.name = name;
      this.input = input.reference();
      this.output = output.reference();
    }

    @Override
    public Outcome execute(Context context) throws Exception {
      context.write(output, new TimestampedInteger(context.read(input).getInteger() + 1));
      System.out.println(name + " Added one!");
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
