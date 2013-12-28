package com.liveramp.megadesk.refactor.gear;

import java.util.List;

import com.liveramp.megadesk.refactor.node.Node;

public interface Gear extends Node {

  public List<Node> reads();

  public List<Node> writes();

  public boolean isRunnable();

  public void run();
}
