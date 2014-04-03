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

package com.liveramp.megadesk.recipes.agent;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestAgent extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(TestAgent.class);


  private static abstract class TransferAgent<INPUT, OUTPUT> implements Agent<INPUT, OUTPUT> {

    private final Variable<INPUT> input;
    private final Variable<OUTPUT> output;

    public TransferAgent(Variable<INPUT> input, Variable<OUTPUT> output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public Transaction<INPUT> readTransaction() {
      return new Transaction<INPUT>() {
        @Override
        public Dependency dependency() {
          return BaseDependency.builder().writes(input).build();
        }

        @Override
        public INPUT run(Context context) throws Exception {
          INPUT inputValue = context.read(input);
          context.write(input, null);
          return inputValue;
        }
      };
    }

    @Override
    public Transaction<Boolean> writeTransaction(final OUTPUT outputValue) {
      return new Transaction<Boolean>() {
        @Override
        public Dependency dependency() {
          return BaseDependency.builder().writes(output).build();
        }

        @Override
        public Boolean run(Context context) throws Exception {
          if (outputValue != null) {
            context.write(output, outputValue);
            return false;
          } else {
            return true;
          }
        }
      };
    }
  }

  private static class IncrementTransferAgent extends TransferAgent<Integer, Integer> implements Function<Integer, Integer> {

    public IncrementTransferAgent(Variable<Integer> input, Variable<Integer> output) {
      super(input, output);
    }

    @Override
    public Function<Integer, Integer> function() {
      return this;
    }

    @Override
    public Integer run(Integer integer) {
      return integer == null ? null : integer + 1;
    }
  }

  @Test
  public void testMain() throws Exception {

    Variable<Integer> a = new InMemoryLocal<Integer>(0);
    Variable<Integer> b = new InMemoryLocal<Integer>(null);
    Variable<Integer> c = new InMemoryLocal<Integer>(null);
    Variable<Integer> d = new InMemoryLocal<Integer>(null);

    Agent<Integer, Integer> agentA = new IncrementTransferAgent(a, b);
    Agent<Integer, Integer> agentB = new IncrementTransferAgent(b, c);
    Agent<Integer, Integer> agentC = new IncrementTransferAgent(c, d);

    AgentExecutor executor = new AgentExecutor();

    executor.run(agentA);
    executor.run(agentB);
    executor.run(agentC);

    executor.join();

    assertEquals(new Integer(3), d.driver().persistence().read());
  }
}
