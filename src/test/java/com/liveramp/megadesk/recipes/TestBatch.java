package com.liveramp.megadesk.recipes;

import com.liveramp.megadesk.test.BaseTestCase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestBatch extends BaseTestCase {

  @Test
  public void testBatching() {

    Batch<Integer, Integer> batch = Batch.getByName("summed-integers", new IntSummer());

    batch.append(3);
    batch.append(2);

    int sum = batch.readBatch();
    assertEquals(5, sum);
    //call to readBatch should freeze the batch until it gets popped
    batch.append(10);
    sum = batch.readBatch();
    assertEquals(5, sum);
    batch.popBatch();
    sum = batch.readBatch();
    assertEquals(10, sum);

    Batch<Integer, Integer> sameBatchNewName = Batch.getByName("summed-integers", new IntSummer());
    int newSum = sameBatchNewName.readBatch();
    assertEquals(10, newSum);
  }

  private static class IntSummer implements Batch.Merger<Integer, Integer> {

    @Override
    public Integer merge(List<Integer> integers) {
      Integer sum = 0;
      for (Integer integer : integers) {
        sum += integer;
      }
      return sum;
    }
  }
}
