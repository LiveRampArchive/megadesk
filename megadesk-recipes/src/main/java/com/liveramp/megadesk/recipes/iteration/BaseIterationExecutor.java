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

package com.liveramp.megadesk.recipes.iteration;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

public class BaseIterationExecutor implements IterationExecutor {

  Logger LOG = Logger.getLogger(BaseIterationExecutor.class);

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final List<Future> remainingTasks = Lists.newArrayList();

  private final class Task implements Runnable {

    private final Iteration iteration;

    public Task(Iteration iteration) {
      this.iteration = iteration;
    }

    @Override
    public void run() {
      Iteration nextIteration;
      try {
        nextIteration = iteration.call();
      } catch (Exception e) {
        LOG.error("Exception while running iteration: " + iteration, e); // TODO
        throw new RuntimeException(e); // TODO
      }
      if (nextIteration != null) {
        execute(nextIteration);
      }
    }
  }

  @Override
  public synchronized void execute(Iteration iteration) {
    clearRemainingTasks();
    remainingTasks.add(executorService.submit(new Task(iteration)));
  }

  @Override
  public void execute(Iteration... iterations) {
    for (Iteration iteration : iterations) {
      execute(iteration);
    }
  }

  @Override
  public void join() throws InterruptedException {
    while (true) {
      if (isEmpty()) {
        return;
      }
      Thread.sleep(1000); // TODO
    }
  }

  public synchronized boolean isEmpty() {
    clearRemainingTasks();
    return remainingTasks.size() == 0;
  }

  private synchronized void clearRemainingTasks() {
    Iterator<Future> iterator = remainingTasks.iterator();
    while (iterator.hasNext()) {
      Future future = iterator.next();
      if (future.isDone()) {
        iterator.remove();
      }
    }
  }
}
