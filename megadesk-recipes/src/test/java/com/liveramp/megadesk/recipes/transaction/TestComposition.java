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

package com.liveramp.megadesk.recipes.transaction;

import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestComposition extends BaseTestCase {

  @Test
  public void testMain() throws Exception {
    TransactionExecutor executor = new BaseTransactionExecutor();

    Variable<Long> v1 = new InMemoryLocal<Long>(0L);
    Variable<Long> v2 = new InMemoryLocal<Long>(0L);
    Variable<Long> v3 = new InMemoryLocal<Long>(0L);

    executor.execute(new Composition(
        new Write<Long>(v1, 1L),
        new Copy<Long>(v1, v2),
        new IncrementLong(v2, 2),
        new Copy<Long>(v2, v3),
        new IncrementLong(v3, 3)
    ));

    assertEquals(1, (long)executor.execute(new Read<Long>(v1)));
    assertEquals(3, (long)executor.execute(new Read<Long>(v2)));
    assertEquals(6, (long)executor.execute(new Read<Long>(v3)));
  }
}
