package com.liveramp.megadesk.curator.state;

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializedPersistence;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;

public class CuratorPersistence<VALUE> extends SerializedPersistence<VALUE> implements Persistence<VALUE> {

  private final NodeCache cache;
  private CuratorFramework curator;
  private final String path;

  public CuratorPersistence(CuratorFramework curator, String path, SerializationHandler<VALUE> serializer) {
    super(serializer);
    this.curator = curator;
    this.path = path;
    this.cache = new NodeCache(curator, path);

    try {
      if (curator.checkExists().forPath(path) == null) {
        curator.create().forPath(path);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeBytes(byte[] serializedObject) {
    try {
      curator.setData().forPath(path, serializedObject);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected byte[] readBytes() {
    return cache.getCurrentData().getData();
  }
}
