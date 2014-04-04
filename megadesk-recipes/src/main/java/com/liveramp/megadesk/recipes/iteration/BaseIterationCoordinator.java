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

package com.liveramp.megadesk.recipes.iteration;

import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.recipes.state.transaction.Alter;
import com.liveramp.megadesk.recipes.state.transaction.Read;
import com.liveramp.megadesk.recipes.state.transaction.Write;

public class BaseIterationCoordinator implements IterationCoordinator {

  private final static Logger LOG = Logger.getLogger(BaseIterationCoordinator.class);

  private final BaseIterationExecutor iterationExecutor = new BaseIterationExecutor();
  private final BaseExecutor transactionExecutor = new BaseExecutor();
  private final String permit = UUID.randomUUID().toString();

  private class CoordinatedIteration implements Iteration {

    private final Iteration iteration;
    private final IterationState state;

    public CoordinatedIteration(Iteration iteration, IterationState state) {
      this.iteration = iteration;
      this.state = state;
    }

    @Override
    public Iteration call() throws Exception {
      return transactionExecutor.execute(new Transaction<Iteration>() {
        @Override
        public Dependency dependency() {
          return BaseDependency.builder().writes(state.iterationLock()).build();
        }

        @Override
        public Iteration run(Context context) throws Exception {
          if (!hasPermit(state)) {
            return null;
          } else {
            Iteration nextIteration = iteration.call();
            if (!hasPermit(state)) {
              // No more permit, just abandon
              return null;
            } else if (nextIteration == null) {
              // Has a permit, but no next iteration, abandon permit and abandon
              removePermit(state);
              return null;
            } else {
              // Has a permit and has a next iteration, execute it
              return new CoordinatedIteration(nextIteration, state);
            }
          }
        }
      });
    }
  }

  @Override
  public void execute(Iteration iteration, IterationState state) throws Exception {
    addPermit(state);
    iterationExecutor.execute(new CoordinatedIteration(iteration, state));
  }

  @Override
  public void shutdown(final IterationState state) throws Exception {
    transactionExecutor.execute(new Write<ImmutableList<String>>(state.iterationPermits(), ImmutableList.<String>of()));
  }

  protected boolean hasPermit(final IterationState state) throws Exception {
    return transactionExecutor.execute(new Read<ImmutableList<String>>(state.iterationPermits())).contains(permit);
  }

  protected void addPermit(final IterationState state) throws Exception {
    transactionExecutor.execute(new Alter<ImmutableList<String>>(state.iterationPermits()) {
      @Override
      protected ImmutableList<String> alter(ImmutableList<String> value) {
        List<String> newPermits = Lists.newArrayList(value);
        newPermits.add(permit);
        return ImmutableList.copyOf(newPermits);
      }
    });
  }

  private void removePermit(IterationState state) throws Exception {
    transactionExecutor.execute(new Alter<ImmutableList<String>>(state.iterationPermits()) {
      @Override
      protected ImmutableList<String> alter(ImmutableList<String> value) {
        List<String> newPermits = Lists.newArrayList(value);
        newPermits.remove(permit);
        return ImmutableList.copyOf(newPermits);
      }
    });
  }

  @Override
  public void execute(Iteration iteration) {
    iterationExecutor.execute(iteration);
  }

  @Override
  public void execute(Iteration... iterations) {
    iterationExecutor.execute(iterations);
  }

  @Override
  public void join() throws InterruptedException {
    iterationExecutor.join();
  }
}
