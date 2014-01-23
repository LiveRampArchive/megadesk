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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestBatch extends BaseTestCase {

  @Test
  public void testBatching() {

    Batch<Integer> batch = Batch.getByName("summed-integers");

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
    Batch<Integer> sameBatchNewName = Batch.getByName("summed-integers");
    List<Integer> newSum = sameBatchNewName.readBatch();
    assertEquals(ImmutableList.of(10), newSum);
  }
}

