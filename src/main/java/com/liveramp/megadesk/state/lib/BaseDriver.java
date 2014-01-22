package com.liveramp.megadesk.state.lib;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.Reference;
import org.apache.commons.lang.builder.CompareToBuilder;

import java.util.concurrent.locks.ReadWriteLock;

public class BaseDriver<VALUE> implements Driver<VALUE> {

  private final ReadWriteLock executionLock;
  private final ReadWriteLock persistenceLock;
  private final Persistence<VALUE> persistence;
  private final Reference<VALUE> reference;

  public BaseDriver(Reference<VALUE> reference, Persistence<VALUE> persistence, ReadWriteLock persistenceLock, ReadWriteLock executionLock) {
    this.reference = reference;
    this.persistence = persistence;
    this.persistenceLock = persistenceLock;
    this.executionLock = executionLock;
  }

  @Override
  public Reference<VALUE> reference() {
    return reference;
  }

  @Override
  public ReadWriteLock executionLock() {
    return executionLock;
  }

  @Override
  public ReadWriteLock persistenceLock() {
    return persistenceLock;
  }

  @Override
  public Persistence<VALUE> persistence() {
    return persistence;
  }

  @Override
  public int compareTo(Driver<VALUE> valueDriver) {
    return new CompareToBuilder().append(reference, valueDriver.reference()).toComparison();
  }
}
