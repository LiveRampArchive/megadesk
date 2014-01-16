package com.liveramp.megadesk.gear;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.lock.Lock;
import com.liveramp.megadesk.node.Node;

public interface OldGear {

  Node getNode();

  Lock getMasterLock();

  GearPersistence getPersistence();

  Dependency getDependency();

  Outcome run() throws Exception;
}
