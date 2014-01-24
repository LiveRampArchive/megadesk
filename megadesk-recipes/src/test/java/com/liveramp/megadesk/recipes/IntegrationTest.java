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

package com.liveramp.megadesk.recipes;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryVariable;
import com.liveramp.megadesk.base.state.Name;
import com.liveramp.megadesk.base.transaction.BaseBinding;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.recipes.gear.ConditionalGear;
import com.liveramp.megadesk.recipes.gear.Gear;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.gear.worker.NaiveWorker;
import com.liveramp.megadesk.recipes.gear.worker.Worker;
import com.liveramp.megadesk.recipes.state.transaction.Alter;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class IntegrationTest extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

  private static class StepGear extends ConditionalGear implements Gear {

    private final List<StepGear> parents;
    private final Variable<Boolean> variable = new InMemoryVariable<Boolean>(false);

    public StepGear(StepGear... parents) {
      this.parents = Arrays.asList(parents);
      setDependency(BaseDependency.<Variable>builder().snapshots(references(parents)).writes(variable).build());
    }

    private static List<Variable> references(StepGear... parents) {
      List<Variable> result = Lists.newArrayList();
      for (StepGear parent : parents) {
        result.add(parent.variable);
      }
      return result;
    }

    @Override
    public Outcome check(Context context) {
      for (StepGear parent : parents) {
        if (!context.read(parent.variable.reference())) {
          return Outcome.STANDBY;
        }
      }
      return Outcome.SUCCESS;
    }

    @Override
    public Outcome execute(Context context) {
      // no-op
      context.write(variable.reference(), true);
      return Outcome.ABANDON;
    }
  }

  private static class TransferGear extends ConditionalGear implements Gear {

    private final Variable<Integer> src;
    private final Variable<Integer> dst;

    private TransferGear(Variable<Integer> src, Variable<Integer> dst) {
      setDependency(BaseDependency.<Variable>builder().writes(src, dst).build());
      this.src = src;
      this.dst = dst;
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
      Accessor<Integer> source = context.accessor(src);
      Accessor<Integer> destination = context.accessor(dst);
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

    final Variable<Integer> A = new InMemoryVariable<Integer>(1);
    final Variable<Integer> B = new InMemoryVariable<Integer>(0);
    final Variable<Integer> C = new InMemoryVariable<Integer>(0);
    final Variable<Integer> D = new InMemoryVariable<Integer>(0);

    Gear gearA = new TransferGear(A, B);
    Gear gearB = new TransferGear(B, C);
    Gear gearC = new TransferGear(C, D);

    worker().complete(gearA, gearB, gearC);

    // Check using a transaction
    assertEquals(true, executor().execute(new Transaction<Boolean>() {

      @Override
      public Dependency<Variable> dependency() {
        return BaseDependency.<Variable>builder()
                   .reads(A, B, C, D).build();
      }

      @Override
      public Boolean run(Context context) throws Exception {
        return context.read(A) == 0
                   && context.read(B) == 0
                   && context.read(C) == 0
                   && context.read(D) == 1;
      }
    }));

    assertEquals(Integer.valueOf(0), A.driver().persistence().read());
    assertEquals(Integer.valueOf(0), B.driver().persistence().read());
    assertEquals(Integer.valueOf(0), C.driver().persistence().read());
    assertEquals(Integer.valueOf(1), D.driver().persistence().read());
  }

  @Test
  public void testSteps() throws Exception {
    StepGear stepA = new StepGear();
    StepGear stepB = new StepGear(stepA);
    StepGear stepC = new StepGear(stepA);
    StepGear stepD = new StepGear(stepB, stepC);

    worker().complete(stepA, stepB, stepC, stepD);

    assertEquals(true, stepA.variable.driver().persistence().read());
    assertEquals(true, stepB.variable.driver().persistence().read());
    assertEquals(true, stepC.variable.driver().persistence().read());
    assertEquals(true, stepD.variable.driver().persistence().read());
  }

  @Test
  public void testBatch() throws Exception {
    final Variable<Integer> A = new InMemoryVariable<Integer>(0);
    final Variable<Integer> B = new InMemoryVariable<Integer>(0);

    Alter<Integer> increment = new Alter<Integer>() {

      @Override
      public Integer alter(Integer value) {
        return value + 1;
      }
    };

    // Increment A twice
    executor().execute(increment, new BaseBinding().bind(new Name<Integer>("input"), A));
    executor().execute(increment, new BaseBinding().bind(new Name<Integer>("input"), A));
    // Transfer A to B
    executor().execute(new TransferGear(A, B));

    assertEquals(Integer.valueOf(0), A.driver().persistence().read());
    assertEquals(Integer.valueOf(2), B.driver().persistence().read());
  }
}
