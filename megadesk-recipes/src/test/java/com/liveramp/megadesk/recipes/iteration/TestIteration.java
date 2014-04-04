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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIteration extends BaseTestCase {

  @Test
  public void testIteration() throws Exception {
    final List<Integer> list = new ArrayList<Integer>();
    Iteration addFive = new Iteration() {
      @Override
      public Iteration call() throws Exception {
        list.add(list.size());
        if (list.size() < 5) {
          return this;
        } else {
          return null;
        }
      }
    };

    BaseIterationExecutor executor = new BaseIterationExecutor();
    executor.execute(addFive);
    executor.join();
    assertEquals(5, list.size());
  }

  @Test
  public void testIterationCoordinator() throws Exception {
    final List<Integer> list = new ArrayList<Integer>();
    Iteration addNeverEnding = new Iteration() {
      @Override
      public Iteration call() throws Exception {
        list.add(list.size());
        return this;
      }
    };

    IterationState state = new BaseIterationState(new InMemoryLocal(), new InMemoryLocal<ImmutableList<String>>(ImmutableList.<String>of()));

    IterationCoordinator coordinator = new BaseIterationCoordinator();
    coordinator.execute(addNeverEnding, state);
    Thread.sleep(1000);
    coordinator.shutdown(state);
    coordinator.join();
    assertTrue(list.size() > 0);
  }
}
