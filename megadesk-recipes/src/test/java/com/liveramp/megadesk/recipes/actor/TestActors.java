package com.liveramp.megadesk.recipes.actor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.core.transaction.Executor;

public class TestActors {

  @Test
  public void testActors() throws Exception {

    SplitEvenAndOdd splitEvenAndOdd = new SplitEvenAndOdd();
    AddFive addFive = new AddFive();
    MultTwenty multTwenty = new MultTwenty();
    Print print = new Print();

    splitEvenAndOdd.setEvens(addFive.rawAddress());
    splitEvenAndOdd.setOdds(multTwenty.rawAddress());

    addFive.addRecipient(print.rawAddress());

    multTwenty.addRecipient(print.rawAddress());


    ThreadPoolExecutor service = new ScheduledThreadPoolExecutor(20);

    splitEvenAndOdd.spawn(service);
    multTwenty.spawn(service);
    addFive.spawn(service);
    print.spawn(service);

    Address address = splitEvenAndOdd.address();

    Executor exec = new BaseExecutor();

    address.send(10);
    address.send(3);
    address.send(5);
    address.send(20);

    service.awaitTermination(100, TimeUnit.SECONDS);


  }

  @Test
  public void testWorkflowFramework() {

    StepActor sendEmail = new StepActor(new Action("email"));
    StepActor makeAMs = new StepActor(new Action("makeAMs"), sendEmail);
    StepActor computeStats = new StepActor(new Action("computeStats"), makeAMs);
    StepActor persistAms = new StepActor(new Action("persistAms"), makeAMs);
    StepActor persistStats = new StepActor(new Action("persistStats"), computeStats);
    StepActor consumeFinish = new StepActor(new Action("finish"), persistAms, persistStats);


    WorkflowDirector workflowDirector = new WorkflowDirector(consumeFinish);
    workflowDirector.run();


  }


  private static class AddFive extends BaseActor<Integer> {

    private final List<RawAddress<? super Integer>> outputs = Lists.newArrayList();

    public void addRecipient(RawAddress<? super Integer> rawAddress) {
      outputs.add(rawAddress);
    }

    @Override
    protected void act(Integer m) {
      Integer largerM = m + 5;
      for (RawAddress<? super Integer> output : outputs) {
        send(output, largerM);
      }
    }
  }

  private static class MultTwenty extends BaseActor<Integer> {

    private final List<RawAddress<? super Integer>> outputs = Lists.newArrayList();

    public void addRecipient(RawAddress<? super Integer> rawAddress) {
      outputs.add(rawAddress);
    }

    @Override
    protected void act(Integer m) {
      Integer largerM = m * 20;
      for (RawAddress<? super Integer> output : outputs) {
        send(output, largerM);
      }
    }
  }

  private static class Print extends BaseActor<Object> {

    @Override
    protected void act(Object m) {
      System.out.println(m);
    }
  }

  private static class SplitEvenAndOdd extends BaseActor<Integer> {

    private RawAddress<Integer> evens;
    private RawAddress<Integer> odds;

    public void setEvens(RawAddress<Integer> evens) {
      this.evens = evens;
    }

    public void setOdds(RawAddress<Integer> odds) {
      this.odds = odds;
    }

    @Override
    protected void act(Integer m) {
      if (m % 2 == 0) {
        send(evens, m);
      } else {
        send(odds, m);
      }
    }
  }

  private static class Action {

    private String name;

    private Action(String name) {
      this.name = name;
    }

    public void execute() {
      System.out.println(name);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private static class StepActor extends BaseActor<String> {

    private Action action;
    private List<StepActor> dependentActors = Lists.newArrayList();
    private List<StepActor> parents;


    private StepActor(Action action, StepActor... previousActors) {
      this.action = action;
      for (StepActor previousActor : previousActors) {
        previousActor.registerDependency(this);
      }
      this.parents = Lists.newArrayList(previousActors);
    }

    private void registerDependency(StepActor actor) {
      dependentActors.add(actor);
    }

    @Override
    protected void act(String m) {
      action.execute();
      for (StepActor dependentActor : dependentActors) {
        send(dependentActor.rawAddress(), "Go!");
      }
    }

    @Override
    public String toString() {
      return action.toString();
    }

    public Set<StepActor> getHeads() {
      if (this.parents.isEmpty()) {
        return Sets.newHashSet(this);
      } else {
        Set<StepActor> heads = Sets.newHashSet();
        for (StepActor parent : parents) {
          heads.addAll(parent.getHeads());
        }
        return heads;
      }
    }

    public Set<StepActor> getDependentTree() {

      Set<StepActor> steps = Sets.newHashSet(this);
      for (StepActor dependentActor : dependentActors) {
        steps.addAll(dependentActor.getDependentTree());
      }
      return steps;

    }
  }

  private static class WorkflowDirector {

    private final List<StepActor> heads;

    public WorkflowDirector(StepActor... tails) {
      heads = Lists.newArrayList();
      for (StepActor tail : tails) {
        heads.addAll(tail.getHeads());
      }

    }

    public void run() {
      Set<StepActor> stepActors = geTree(heads);
      ThreadPoolExecutor service = new ScheduledThreadPoolExecutor(stepActors.size());
      for (StepActor stepActor : stepActors) {
        System.out.println("starting " + stepActor);
        stepActor.spawn(service);
      }
      for (StepActor head : heads) {
        head.address().send("Go!");
      }
      try {
        service.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

    }

    private Set<StepActor> geTree(List<StepActor> heads) {
      Set<StepActor> all = Sets.newHashSet();
      for (StepActor head : heads) {
        all.addAll(head.getDependentTree());
      }
      return all;
    }
  }

}
