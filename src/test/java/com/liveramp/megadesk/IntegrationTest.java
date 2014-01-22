/**
 *  Copyright 2014 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.megadesk;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.megadesk.gear.ConditionalGear;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.lib.InMemoryDriver;
import com.liveramp.megadesk.test.BaseTestCase;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.Binding;
import com.liveramp.megadesk.transaction.Context;
import com.liveramp.megadesk.transaction.Dependency;
import com.liveramp.megadesk.transaction.Executor;
import com.liveramp.megadesk.transaction.Transaction;
import com.liveramp.megadesk.transaction.lib.Alter;
import com.liveramp.megadesk.worker.NaiveWorker;
import com.liveramp.megadesk.worker.Worker;

import static org.junit.Assert.assertEquals;

public class IntegrationTest extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

  private TestingServer testingServer;

  @Before
  public void setUpDriver() throws Exception {
    this.testingServer = new TestingServer(12000);
  }

  @After
  public void tearDownDriver() throws IOException {
    this.testingServer.close();
  }

  private static class StepGear extends ConditionalGear implements Gear {

    private final List<StepGear> parents;
    private final Driver<Boolean> driver = new InMemoryDriver<Boolean>(false);

    public StepGear(StepGear... parents) {
      this.parents = Arrays.asList(parents);
      setDependency(BaseDependency.<Driver>builder().snapshots(drivers(parents)).writes(driver).build());
    }

    private static List<Driver> drivers(StepGear... parents) {
      List<Driver> result = Lists.newArrayList();
      for (StepGear parent : parents) {
        result.add(parent.driver);
      }
      return result;
    }

    @Override
    public Outcome check(Context context) {
      for (StepGear parent : parents) {
        if (!context.read(parent.driver.reference())) {
          return Outcome.STANDBY;
        }
      }
      return Outcome.SUCCESS;
    }

    @Override
    public Outcome execute(Context context) {
      // no-op
      context.write(driver.reference(), true);
      return Outcome.ABANDON;
    }
  }

  private static class TransferGear extends ConditionalGear implements Gear {

    private final Reference<Integer> src;
    private final Reference<Integer> dst;

    private TransferGear(Driver<Integer> src, Driver<Integer> dst) {
      super(BaseDependency.<Driver>builder().writes(src, dst).build());
      this.src = src.reference();
      this.dst = dst.reference();
    }

    @Override
    public Outcome check(Context context) {
      if (context.read(src) > 0 && context.read(dst) == 0) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    }

    @Override
    public Outcome execute(Context context) {
      Binding<Integer> source = context.binding(src);
      Binding<Integer> destination = context.binding(dst);
      destination.write(source.read());
      source.write(0);
      return Outcome.ABANDON;
    }
  }

  private Worker worker() {
    return new NaiveWorker();
  }

  private Executor executor() {
    return new BaseExecutor();
  }

  @Test
  public void testState() throws Exception {

    final Driver<Integer> driverA = new InMemoryDriver<Integer>(1);
    final Driver<Integer> driverB = new InMemoryDriver<Integer>(0);
    final Driver<Integer> driverC = new InMemoryDriver<Integer>(0);
    final Driver<Integer> driverD = new InMemoryDriver<Integer>(0);

    Gear gearA = new TransferGear(driverA, driverB);
    Gear gearB = new TransferGear(driverB, driverC);
    Gear gearC = new TransferGear(driverC, driverD);

    worker().complete(gearA, gearB, gearC);

    // Check using a transaction
    assertEquals(true, executor().execute(new Transaction<Boolean>() {

      @Override
      public Dependency<Driver> dependency() {
        return BaseDependency.<Driver>builder().reads(driverA, driverB, driverC, driverD).build();
      }

      @Override
      public Boolean run(Context context) throws Exception {
        return context.read(driverA.reference()) == 0
                   && context.read(driverB.reference()) == 0
                   && context.read(driverC.reference()) == 0
                   && context.read(driverD.reference()) == 1;
      }
    }));

    assertEquals(Integer.valueOf(0), driverA.persistence().read());
    assertEquals(Integer.valueOf(0), driverB.persistence().read());
    assertEquals(Integer.valueOf(0), driverC.persistence().read());
    assertEquals(Integer.valueOf(1), driverD.persistence().read());
  }

  @Test
  public void testSteps() throws Exception {
    StepGear stepA = new StepGear();
    StepGear stepB = new StepGear(stepA);
    StepGear stepC = new StepGear(stepA);
    StepGear stepD = new StepGear(stepB, stepC);

    worker().complete(stepA, stepB, stepC, stepD);

    assertEquals(true, stepA.driver.persistence().read());
    assertEquals(true, stepB.driver.persistence().read());
    assertEquals(true, stepC.driver.persistence().read());
    assertEquals(true, stepD.driver.persistence().read());
  }

  @Test
  public void testBatch() throws Exception {
    final Driver<Integer> driverA = new InMemoryDriver<Integer>(0);
    final Driver<Integer> driverB = new InMemoryDriver<Integer>(0);

    Alter<Integer> increment = new Alter<Integer>() {
      @Override
      public Integer alter(Integer value) {
        return value + 1;
      }
    };

    // Increment A twice
    executor().execute(increment, driverA);
    executor().execute(increment, driverA);
    // Transfer A to B
    executor().execute(new TransferGear(driverA, driverB));

    assertEquals(Integer.valueOf(0), driverA.persistence().read());
    assertEquals(Integer.valueOf(2), driverB.persistence().read());
  }
}
