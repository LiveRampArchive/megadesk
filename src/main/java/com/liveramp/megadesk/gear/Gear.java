package com.liveramp.megadesk.gear;

import java.util.List;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.persistence.Persistence;

public interface Gear {

  Node getNode();

  Persistence getPersistence();

  List<Node> reads();

  List<Node> writes();

  boolean isRunnable();

  Outcome run() throws Exception;
}
