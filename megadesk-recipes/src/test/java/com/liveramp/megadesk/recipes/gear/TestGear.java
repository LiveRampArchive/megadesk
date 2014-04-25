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

package com.liveramp.megadesk.recipes.gear;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.base.transaction.Bind;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Accessor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.recipes.iteration.BaseIterationExecutor;
import com.liveramp.megadesk.recipes.iteration.IterationExecutor;
import com.liveramp.megadesk.recipes.transaction.Alter;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestGear extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(TestGear.class);
  private final IterationExecutor iterationExecutor = new BaseIterationExecutor();
  private final TransactionExecutor executor = new BaseTransactionExecutor();

  private static class StepGear extends ConditionalGear implements Gear {

    private final List<StepGear> parents;
    private final Variable<Boolean> variable = new InMemoryLocal<Boolean>(false);

    public StepGear(StepGear... parents) {
      this.parents = Arrays.asList(parents);
      setDependency(BaseDependency.builder().reads(variables(parents)).writes(variable).build());
    }

    private static List<Variable> variables(StepGear... parents) {
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
      setDependency(BaseDependency.builder().writes(src, dst).build());
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

  private IterationExecutor iterationExecutor() {
    return iterationExecutor;
  }

  private TransactionExecutor executor() {
    return executor;
  }

  @Test
  public void testState() throws Exception {

    final Variable<Integer> A = new InMemoryLocal<Integer>(1);
    final Variable<Integer> B = new InMemoryLocal<Integer>(0);
    final Variable<Integer> C = new InMemoryLocal<Integer>(0);
    final Variable<Integer> D = new InMemoryLocal<Integer>(0);

    Gear gearA = new TransferGear(A, B);
    Gear gearB = new TransferGear(B, C);
    Gear gearC = new TransferGear(C, D);

    iterationExecutor().execute(
        new BaseGearIteration(gearA),
        new BaseGearIteration(gearB),
        new BaseGearIteration(gearC));

    iterationExecutor().join();

    // Check using a transaction
    assertEquals(true, executor().execute(new Transaction<Boolean>() {

      @Override
      public Dependency dependency() {
        return BaseDependency.builder()
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

    iterationExecutor().execute(
        new BaseGearIteration(stepA),
        new BaseGearIteration(stepB),
        new BaseGearIteration(stepC),
        new BaseGearIteration(stepD));

    iterationExecutor().join();

    assertEquals(true, stepA.variable.driver().persistence().read());
    assertEquals(true, stepB.variable.driver().persistence().read());
    assertEquals(true, stepC.variable.driver().persistence().read());
    assertEquals(true, stepD.variable.driver().persistence().read());
  }

  @Test
  public void testBatch() throws Exception {
    final Variable<Integer> A = new InMemoryLocal<Integer>(0);
    final Variable<Integer> B = new InMemoryLocal<Integer>(0);

    Alter<Integer> increment = new Alter<Integer>() {

      @Override
      public Integer alter(Integer value) {
        return value + 1;
      }
    };

    // Increment A twice
    executor().execute(increment, new Bind(A));
    executor().execute(increment, new Bind(A));
    // Transfer A to B
    executor().execute(new TransferGear(A, B));

    assertEquals(Integer.valueOf(0), A.driver().persistence().read());
    assertEquals(Integer.valueOf(2), B.driver().persistence().read());
  }
}
