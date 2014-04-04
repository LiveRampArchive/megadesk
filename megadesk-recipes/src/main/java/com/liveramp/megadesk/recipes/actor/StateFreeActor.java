package com.liveramp.megadesk.recipes.actor;

public abstract class StateFreeActor<Message> extends Actor<Void, Message> {

  protected StateFreeActor(String name) {
    super(name, null);
  }

  @Override
  protected Void act(Void aVoid, Message m) {
    this.act(m);
    return null;
  }

  protected abstract void act(Message m);
}
