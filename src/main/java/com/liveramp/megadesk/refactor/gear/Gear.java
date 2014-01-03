package com.liveramp.megadesk.refactor.gear;

import java.util.List;

import com.liveramp.megadesk.refactor.attempt.Outcome;
import com.liveramp.megadesk.refactor.node.Node;
import com.liveramp.megadesk.refactor.persistence.Persistence;

public interface Gear {

  Node getNode();

  Persistence getPersistence();

  List<Node> reads();

  List<Node> writes();

  boolean isRunnable();

  Outcome run() throws Exception;
}
