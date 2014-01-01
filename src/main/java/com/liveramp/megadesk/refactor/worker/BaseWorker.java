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

package com.liveramp.megadesk.refactor.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import com.liveramp.megadesk.refactor.gear.Gear;

public class BaseWorker implements Worker {

  private static final Logger LOG = Logger.getLogger(BaseWorker.class);
  private static final int DEFAULT_MAX_THREAD_POOL_SIZE = 1 << 10;
  private final ExecutorService executor;

  public BaseWorker() {
    executor = Executors.newFixedThreadPool(DEFAULT_MAX_THREAD_POOL_SIZE, new ExecutorThreadFactory());
  }

  private static class ExecutorThreadFactory implements ThreadFactory {

    private int threadId = 0;

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "Base Worker Thread " + this + "#" + threadId++);
    }
  }

  private static class ExecutorTask implements Runnable {

    private final Gear gear;

    public ExecutorTask(Gear gear) {
      this.gear = gear;
    }

    @Override
    public void run() {

      while (true) {

        // TODO

        try {
          LOG.info("Gear " + gear + " sleeping...");
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          LOG.info("Gear " + gear + " stopped");
          return;
        }
      }
    }
  }

  @Override
  public void run(Gear gear) {
    executor.execute(new ExecutorTask(gear));
  }
}
