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

package com.liveramp.megadesk.executor;

import com.liveramp.megadesk.dependency.DependencyWatcher;
import com.liveramp.megadesk.step.Step;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Executor {

  private static final int DEFAULT_MAX_THREAD_POOL_SIZE = 1;

  private final ThreadPoolExecutor executor;

  public Executor() {
    this(DEFAULT_MAX_THREAD_POOL_SIZE);
  }

  public Executor(int maxThreadPoolSize) {
    this.executor = new ThreadPoolExecutor(
        0,
        maxThreadPoolSize,
        1,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ExecutorThreadFactory());
  }

  public void execute(Step step) {
    executor.execute(new ExecutorTask(step));
  }

  private static class ExecutorThreadFactory implements ThreadFactory {

    private int threadId = 0;

    @Override
    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, "Executor thread #" + threadId++);
    }
  }

  private class ExecutorTask implements Runnable {

    private final Step step;

    public ExecutorTask(Step step) {
      this.step = step;
    }

    @Override
    public void run() {
      ExecutorTaskDependencyWatcher watcher = new ExecutorTaskDependencyWatcher();
      try {
        if (step.acquire(watcher)) {
          try {
            step.execute();
            step.release();
          } catch (Throwable t) {
            // TODO
          }
        } else {
          watcher.activate();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private class ExecutorTaskDependencyWatcher implements DependencyWatcher {

      private boolean changed = false;
      private boolean activated = false;

      @Override
      public synchronized void onDependencyChange() {
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
        Executor.this.execute(step);
        activated = false;
      }
    }
  }
}
