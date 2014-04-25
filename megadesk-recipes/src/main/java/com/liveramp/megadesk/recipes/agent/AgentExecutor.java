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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;

public class AgentExecutor {

  private final ExecutorService executor = Executors.newFixedThreadPool(100);
  private final TransactionExecutor transactionExecutor = new BaseTransactionExecutor();

  public void join() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  private class Task<INPUT, OUTPUT> implements Runnable {

    private final Agent<INPUT, OUTPUT> agent;

    public Task(Agent<INPUT, OUTPUT> agent) {
      this.agent = agent;
    }

    @Override
    public void run() {
      try {
        Boolean running = true;
        while (running) {
          INPUT input = transactionExecutor.execute(agent.readTransaction());
          Function<INPUT, OUTPUT> function = agent.function();
          OUTPUT output = function.run(input);
          running = transactionExecutor.execute(agent.writeTransaction(output));
        }
      } catch (Exception e) {
        throw new RuntimeException(e); // TODO
      }
    }
  }

  public <INPUT, OUTPUT> void run(Agent<INPUT, OUTPUT> agent) throws Exception {
    executor.execute(new Task<INPUT, OUTPUT>(agent));
  }
}
