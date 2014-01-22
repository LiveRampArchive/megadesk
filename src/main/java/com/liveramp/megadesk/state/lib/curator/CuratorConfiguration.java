package com.liveramp.megadesk.state.lib.curator;

import com.liveramp.megadesk.state.lib.filesystem_tools.JavaSerialization;
import com.liveramp.megadesk.state.lib.filesystem_tools.PathMaker;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import org.apache.curator.framework.CuratorFramework;

public enum CuratorConfiguration {
  INSTANCE;

  private PathMaker pathMaker = new PathMaker.Default("");
  private SerializationHandler serializer = new JavaSerialization();
  private CuratorFramework framework;

  private CuratorConfiguration() {
  }

  public PathMaker getPathMaker() {
    return pathMaker;
  }

  public void setPathMaker(PathMaker pathMaker) {
    this.pathMaker = pathMaker;
  }

  public SerializationHandler getSerializer() {
    return serializer;
  }

  public void setSerializer(SerializationHandler serializer) {
    this.serializer = serializer;
  }

  public CuratorFramework getFramework() {
    return framework;
  }

  public void setFramework(CuratorFramework framework) {
    this.framework = framework;
  }
}


