package com.liveramp.megadesk.gear;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.lock.Lock;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.persistence.Persistence;

public interface Gear {

  Node getNode();

  Lock getMasterLock();

  Persistence getPersistence();

  Dependency getDependency();

  Outcome run() throws Exception;
}
