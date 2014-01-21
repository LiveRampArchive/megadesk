package com.liveramp.megadesk.recipes;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.test.BaseTestCase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestBatch extends BaseTestCase {

  @Test
  public void testBatching() {

    Batch<Integer> batch = Batch.getByName("summed-integers");

    batch.append(3);
    batch.append(2);

    List<Integer> list = batch.readBatch();
    assertEquals(ImmutableList.of(3, 2), list);
    //call to readBatch should freeze the batch until it gets popped
    batch.append(10);
    list = batch.readBatch();
    assertEquals(ImmutableList.of(3, 2), list);
    batch.popBatch();
    list = batch.readBatch();
    assertEquals(ImmutableList.of(10), list);

    Batch<Integer> sameBatchNewName = Batch.getByName("summed-integers");
    List<Integer> newSum = sameBatchNewName.readBatch();
    assertEquals(ImmutableList.of(10), newSum);
  }
}
