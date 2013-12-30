package com.liveramp.megadesk.refactor.gear;

import java.util.List;

import com.liveramp.megadesk.refactor.node.Node;

public interface Gear extends Node {

  List<Node> reads();

  List<Node> writes();

  boolean isRunnable();

  void run() throws Exception;
}
