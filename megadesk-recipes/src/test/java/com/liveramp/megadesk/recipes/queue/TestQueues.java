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

package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.liveramp.megadesk.base.state.InMemoryDriver;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;
import com.liveramp.megadesk.test.BaseTestCase;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQueues extends BaseTestCase {

  @Test
  public void testBatching() {

    DriverFactory factory = new BasicFactory();
    BaseExecutor executor = new BaseExecutor();
    BatchStructure<Integer> batch = BatchStructure.getByName("summed-integers", factory, executor);

    //basic batching
    batch.append(3);
    batch.append(2);
    List<Integer> list = batch.readBatch();
    assertEquals(ImmutableList.of(3, 2), list);

    //call to readBatch should freeze the batch until it gets popped
    batch.append(10);
    list = batch.readBatch();
    assertEquals(ImmutableList.of(3, 2), list);

    //pop, then read should give us the value appended earlier
    batch.popBatch();
    list = batch.readBatch();
    assertEquals(ImmutableList.of(10), list);

    //Batches with the same name are the same
    BatchStructure<Integer> sameBatchNewName = BatchStructure.getByName("summed-integers", factory, executor);
    List<Integer> newSum = sameBatchNewName.readBatch();
    assertEquals(ImmutableList.of(10), newSum);

    sameBatchNewName.popBatch();

    //Reading an empty batch should still freeze the batch until pop is called
    assertTrue(sameBatchNewName.readBatch().isEmpty());
    sameBatchNewName.append(10);
    assertTrue(sameBatchNewName.readBatch().isEmpty());
  }

  @Test
  public void testQueue() {

  }

  private static class BasicFactory implements DriverFactory {

    private static Map<String, Driver> drivers = Maps.newHashMap();

    @Override
    public <T> Driver<T> get(String referenceName, T intialValue) {
      if (!drivers.containsKey(referenceName)) {
        drivers.put(referenceName, new InMemoryDriver(intialValue));
      }
      return drivers.get(referenceName);
    }
  }
}

