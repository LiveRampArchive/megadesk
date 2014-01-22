package com.liveramp.megadesk.state.lib.curator;

import com.liveramp.megadesk.state.lib.filesystem_tools.PathMaker;
import org.apache.curator.framework.CuratorFramework;

public enum CuratorConfiguration {
  INSTANCE;

  private PathMaker pathMaker = new PathMaker.Default("");
  private CuratorFramework framework;

  private CuratorConfiguration() {
  }

  public PathMaker getPathMaker() {
    return pathMaker;
  }

  public void setPathMaker(PathMaker pathMaker) {
    this.pathMaker = pathMaker;
  }

  public CuratorFramework getFramework() {
    return framework;
  }

  public void setFramework(CuratorFramework framework) {
    this.framework = framework;
  }
}


