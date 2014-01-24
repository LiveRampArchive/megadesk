package com.liveramp.megadesk.curator.state;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.ConditionalGear;
import com.liveramp.megadesk.recipes.gear.Gear;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.gear.worker.NaiveWorker;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;
import com.liveramp.megadesk.recipes.queue.Queue;
import com.liveramp.megadesk.recipes.queue.QueueExecutable;
import com.liveramp.megadesk.recipes.state.persistence.JavaObjectSerialization;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.io.StreamCorruptedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Demo {

  public static void workflowStep(final String name, String... next) throws InterruptedException {

    CuratorFramework framework = CuratorFrameworkFactory.newClient("zk01:2181,zk02:2181,zk03:2181", new RetryOneTime(100));
    framework.start();
    DriverFactory factory = new Demo.CuratorFactory(framework, false);
    BaseExecutor executor = new BaseExecutor();
    QueueExecutable<Integer> input = QueueExecutable.getQueueByName(name, factory, executor);
    List<Queue<Integer>> outputs = Lists.newArrayList();
    for (String s : next) {
      outputs.add(QueueExecutable.<Integer>getQueueByName(s, factory, executor).getQueue());
    }

    Speak.Speaker speaker = new Speak.Speaker() {
      @Override
      public String say(Integer i) {
        return name + " running on import " + i;
      }
    };
    Speak speak = new Speak(speaker, input.getQueue(), outputs);

    NaiveWorker worker = new NaiveWorker();
    worker.run(speak);

    worker.join();
  }

  public static void demoClient(String queuename) throws InterruptedException {
    CuratorFramework framework = CuratorFrameworkFactory.newClient("zk01:2181,zk02:2181,zk03:2181", new RetryOneTime(100));
    framework.start();
    DriverFactory factory = new Demo.CuratorFactory(framework, false);
    BaseExecutor executor = new BaseExecutor();
    QueueExecutable<Integer> input = QueueExecutable.getQueueByName(queuename, factory, executor);

    Scanner scanner = new Scanner(System.in);
    while (scanner.hasNextLine()) {
      input.append(scanner.nextInt());
    }
  }

  public static void main(String[] args) throws InterruptedException {
    if (args[0].equals("client")) {
      demoClient(args[1]);
    } else {
      workflowStep(args[0], Arrays.copyOfRange(args, 1, args.length));
    }
  }

  public static class CuratorFactory implements DriverFactory {
    private static Map<String, Driver> drivers = Maps.newHashMap();
    private final CuratorFramework framework;
    private boolean reset;

    private CuratorFactory(CuratorFramework framework, boolean reset) {
      this.framework = framework;
      this.reset = reset;
    }

    @Override
    public <T> Driver<T> get(String referenceName, T intialValue) {
      if (!drivers.containsKey(referenceName)) {
        drivers.put(referenceName, makeDriver(referenceName, intialValue));
      }
      return drivers.get(referenceName);
    }

    private <T> Driver makeDriver(String referenceName, T intialValue) {
      Driver<T> driver = CuratorDriver.<T>build("/megadesk/demo/" + referenceName, framework, new JavaObjectSerialization<T>());
      try {
        if (reset || driver.persistence().read() == null) {
          driver.persistence().write(intialValue);
        }
      } catch (Exception e) {
        if (e.getCause() instanceof StreamCorruptedException) {
          driver.persistence().write(intialValue);
        }
      }
      return driver;
    }
  }

  public static class Speak extends ConditionalGear implements Gear {

    public static interface Speaker {
      public String say(Integer i);
    }

    private final Queue<Integer> inputQueue;
    private final List<Queue<Integer>> outputQueues;
    private final Speaker speaker;

    private Speak(Speaker speaker, Queue<Integer> inputQueue, List<Queue<Integer>> outputQueues) {
      this.inputQueue = inputQueue;
      this.speaker = speaker;
      this.outputQueues = outputQueues;
      List<Variable> writes = Lists.newArrayList();
      writes.add(inputQueue.getFrozen());
      writes.add(inputQueue.getOutput());
      writes.add(inputQueue.getInput());
      for (Queue<Integer> queue : outputQueues) {
        writes.add(queue.getInput());
      }
      BaseDependency.Builder<Variable> builder = BaseDependency.<Variable>builder()
          .writes(writes);
      this.setDependency(builder.build());
      System.out.println(this.dependency().reads());
      System.out.println(this.dependency().writes());
    }

    @Override
    public Outcome check(Context context) {
      if (inputQueue.read(context) != null) {
        return Outcome.SUCCESS;
      } else {
        inputQueue.pop(context);
        return Outcome.STANDBY;
      }
    }

    @Override
    public Outcome execute(Context context) throws Exception {
      Integer integer = inputQueue.read(context);
      String say = speaker.say(integer);
      Runtime.getRuntime().exec("/usr/bin/say \"" + say + "\"");
      Thread.sleep(3000);
      inputQueue.pop(context);
      for (Queue<Integer> outputQueue : outputQueues) {
        outputQueue.append(context, integer);
      }
      return Outcome.SUCCESS;
    }
  }
}
