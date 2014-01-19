package com.liveramp.megadesk.old.gear;

import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.old.dependency.Dependency;
import com.liveramp.megadesk.old.lock.Lock;
import com.liveramp.megadesk.old.node.Node;

public interface OldGear {

  Node getNode();

  Lock getMasterLock();

  GearPersistence getPersistence();

  Dependency getDependency();

  Outcome run() throws Exception;
}
