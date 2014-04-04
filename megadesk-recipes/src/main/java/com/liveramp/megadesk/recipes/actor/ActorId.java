package com.liveramp.megadesk.recipes.actor;

public class ActorId {

  private final String id;

  public ActorId(String id) {
    this.id = id;
  }

  public ActorId subId(String subID) {
    return new ActorId(id + ":" + subID);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ActorId actorId = (ActorId)o;

    if (id != null ? !id.equals(actorId.id) : actorId.id != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
