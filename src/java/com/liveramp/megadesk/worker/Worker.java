/**
 *  Copyright 2012 LiveRamp
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

package com.liveramp.megadesk.worker;

import com.liveramp.megadesk.condition.ConditionWatcher;
import com.liveramp.megadesk.step.Step;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Worker {

  private static final Logger LOGGER = Logger.getLogger(Worker.class);
  // Default is quasi unbounded
  private static final int DEFAULT_MAX_THREAD_POOL_SIZE = 1 << 10;

  private final ThreadPoolExecutor executor;

  public Worker() {
    this(DEFAULT_MAX_THREAD_POOL_SIZE);
  }

  public Worker(int maxThreadPoolSize) {
    this.executor = new ThreadPoolExecutor(
        0,
        maxThreadPoolSize,
        1,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new WorkerThreadFactory());
  }

  public void execute(Step step) {
    executor.execute(new ExecutorTask(step));
  }

  private static class WorkerThreadFactory implements ThreadFactory {

    private int threadId = 0;

    @Override
    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, "Worker thread #" + threadId++);
    }
  }

  private class ExecutorTask implements Runnable {

    private final Step step;

    public ExecutorTask(Step step) {
      this.step = step;
    }

    @Override
    public void run() {
      ExecutorTaskWatcher watcher = new ExecutorTaskWatcher();
      try {
        if (step.acquire(watcher)) {
          try {
            step.run();
            step.release();
          } catch (Throwable t) {
            LOGGER.error("Step '" + step.getId() + "' encountered throwable: ", t);
            // TODO
          }
        } else {
          watcher.activate();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private class ExecutorTaskWatcher implements ConditionWatcher {

      private boolean changed = false;
      private boolean activated = false;

      @Override
      public synchronized void onChange() {
        changed = true;
        if (activated) {
          execute();
        }
      }

      public synchronized void activate() {
        activated = true;
        if (changed) {
          execute();
        }
      }

      private synchronized void execute() {
        LOGGER.info("Waking up step '" + step.getId() + "'.");
        Worker.this.execute(step);
        activated = false;
      }
    }
  }
}
