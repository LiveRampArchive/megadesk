package com.liveramp.megadesk.recipes.actor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

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

    address.send(10);
    address.send(3);
    address.send(5);
    address.send(20);

    service.awaitTermination(2, TimeUnit.SECONDS);


  }

  @Test
  public void testWorkflowFramework() {

    StepActor sendEmail = new StepActor(new Action("email"));
    StepActor makeAMs = new StepActor(new Action2("makeAMs"), sendEmail);
    StepActor computeStats = new StepActor(new Action("computeStats"), makeAMs);
    StepActor persistAms = new StepActor(new Action2("persistAms"), makeAMs);
    StepActor persistStats = new StepActor(new Action("persistStats"), computeStats);
    StepActor consumeFinish = new StepActor(new Action("finish"), persistAms, persistStats);


    WorkflowDirector workflowDirector = new WorkflowDirector(consumeFinish);
    workflowDirector.run();


  }


  private static class AddFive extends Actor<Void, Integer> {

    private final List<RawAddress<? super Integer>> outputs = Lists.newArrayList();

    protected AddFive() {
      super("add", null);
    }

    public void addRecipient(RawAddress<? super Integer> rawAddress) {
      outputs.add(rawAddress);
    }

    @Override
    protected Void act(Void state, Integer m) {
      Integer largerM = m + 5;
      for (RawAddress<? super Integer> output : outputs) {
        send(output, largerM);
      }
      return null;
    }
  }

  class KeepEvensSendOdds extends Actor<Set<Integer>, Integer> {

    private final RawAddress<Integer> recipient;

    protected KeepEvensSendOdds(String uniqueName, RawAddress<Integer> recipient) {
      super(uniqueName, Sets.<Integer>newHashSet());
      this.recipient = recipient;
    }

    @Override
    protected Set<Integer> act(Set<Integer> state, Integer m) {
      if (m % 2 == 0) {
        state.add(m);
      } else {
        send(recipient, m);
      }
      return state;
    }
  }

  private static class MultTwenty extends Actor<Void, Integer> {

    private final List<RawAddress<? super Integer>> outputs = Lists.newArrayList();

    public void addRecipient(RawAddress<? super Integer> rawAddress) {
      outputs.add(rawAddress);
    }

    private MultTwenty() {
      super("mult", null);
    }

    @Override
    protected Void act(Void state, Integer m) {
      Integer largerM = m * 20;
      for (RawAddress<? super Integer> output : outputs) {
        send(output, largerM);
      }
      return null;
    }
  }

  private static class Print extends Actor<Void, Object> {

    protected Print() {
      super("print", null);
    }

    @Override
    protected Void act(Void state, Object m) {
      System.out.println(m);
      return null;
    }
  }

  private static class SplitEvenAndOdd extends StateFreeActor<Integer> {

    private RawAddress<Integer> evens;
    private RawAddress<Integer> odds;

    protected SplitEvenAndOdd() {
      super("split");
    }

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

    public String name;

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

    public List<Action> subActions() {
      return Lists.newArrayList();
    }
  }

  private static class Action2 extends Action {

    private Action2(String name) {
      super(name);
    }

    public void execute() {
      System.out.println(name);
    }

    @Override
    public String toString() {
      return name;
    }

    public List<Action> subActions() {
      return Lists.newArrayList(new Action(name + "2"));
    }
  }

  private static class StepActor extends Actor<Map<ActorId, Integer>, ActorId> {

    private Action action;
    private List<StepActor> dependentActors = Lists.newArrayList();
    private List<StepActor> parents;
    private List<StepActor> subSteps = Lists.newArrayList();


    private StepActor(Action action, StepActor... previousActors) {
      super(action.name, Maps.<ActorId, Integer>newHashMap());
      this.action = action;
      if (!action.subActions().isEmpty()) {
        for (Action subAction : action.subActions()) {
          StepActor subStep = new StepActor(subAction);
          subSteps.add(subStep);
          subStep.registerDependency(this);
        }
      }
      for (StepActor previousActor : previousActors) {
        previousActor.registerDependency(this);
      }
      this.parents = Lists.newArrayList(previousActors);
    }

    private void registerDependency(StepActor actor) {
      dependentActors.add(actor);
    }

    @Override
    protected Map<ActorId, Integer> act(Map<ActorId, Integer> state, ActorId m) {
      if (!state.containsKey(m)) {
        state.put(m, 0);
      }
      state.put(m, state.get(m) + 1);
      if (atLeastOneMessageFromEach(parents, state)) {
        subtractOneFromEach(parents, state);
        action.execute();
        for (StepActor subStep : subSteps) {
          send(subStep.rawAddress(), this.getActorId());
        }
      }
      if (atLeastOneMessageFromEach(subSteps, state)) {
        subtractOneFromEach(subSteps, state);
        for (StepActor dependentActor : dependentActors) {
          send(dependentActor.rawAddress(), this.getActorId());
        }
      }
      return state;
    }

    private void subtractOneFromEach(List<StepActor> steps, Map<ActorId, Integer> state) {
      for (StepActor step : steps) {
        state.put(step.getActorId(), state.get(step.getActorId()) - 1);
      }
    }

    private boolean atLeastOneMessageFromEach(List<StepActor> steps, Map<ActorId, Integer> state) {
      for (StepActor step : steps) {
        Integer count = state.get(step.getActorId());
        if (count == null || count == 0) {
          return false;
        }
      }
      return true;
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

    public Set<StepActor> getFullTree() {

      Set<StepActor> actors = Sets.newHashSet();
      List<StepActor> queue = Lists.newArrayList();
      queue.add(this);
      while (!queue.isEmpty()) {
        StepActor current = queue.get(0);
        bfs(actors, queue, current.subSteps);
        bfs(actors, queue, current.parents);
        bfs(actors, queue, current.dependentActors);
        queue.remove(0);
      }
      return actors;
    }

    private void bfs(Set<StepActor> actors, List<StepActor> queue, List<StepActor> subSteps1) {
      for (StepActor subStep : subSteps1) {
        if (!actors.contains(subStep)) {
          actors.add(subStep);
          queue.add(subStep);
        }
      }
    }

    private void add(Collection<StepActor> steps, List<StepActor> subSteps1) {
      for (StepActor subStep : subSteps1) {
        if (!steps.contains(subStep)) {
          steps.addAll(subStep.getFullTree());
        }
      }
    }
  }

  private static class WorkflowDirector {

    private final Set<StepActor> heads;
    private final Set<StepActor> all;


    public WorkflowDirector(StepActor... tails) {
      heads = Sets.newHashSet();
      all = Sets.newHashSet();
      for (StepActor tail : tails) {
        heads.addAll(tail.getHeads());
        all.addAll(tail.getFullTree());
      }
    }

    public void run() {
      ThreadPoolExecutor service = new ScheduledThreadPoolExecutor(all.size());
      for (StepActor stepActor : all) {
        stepActor.spawn(service);
      }
      for (StepActor head : heads) {
        head.address().send(new ActorId("Go!"));
      }
      try {
        service.awaitTermination(100, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

    }

    private Set<StepActor> getTree(List<StepActor> heads) {
      Set<StepActor> all = Sets.newHashSet();
      for (StepActor head : heads) {
        all.addAll(head.getFullTree());
      }
      return all;
    }
  }

}
