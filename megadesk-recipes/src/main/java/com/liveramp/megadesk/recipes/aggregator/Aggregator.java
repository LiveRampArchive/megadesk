package com.liveramp.megadesk.recipes.aggregator;

public interface Aggregator<Aggregate, Value> {

  public Aggregate initialValue();

  public Aggregate partialAggregate(Aggregate aggregate, Value newValue);

  public Aggregate finalAggregate(Aggregate finalAggregate, Aggregate partialAggregate);

}
