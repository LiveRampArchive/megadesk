package com.liveramp.megadesk.state.lib.curator;

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;

import java.io.IOException;

public class CuratorPersistence<VALUE> implements Persistence<VALUE> {

  private final NodeCache cache;
  private final SerializationHandler<VALUE> serializer;
  private CuratorFramework curator;
  private final String path;

  public CuratorPersistence(CuratorFramework curator, String path, SerializationHandler<VALUE> serializer) {
    this.curator = curator;
    this.path = path;
    this.cache = new NodeCache(curator, path);
    this.serializer = serializer;

    try {
      if (curator.checkExists().forPath(path) == null) {
        curator.create().forPath(path);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Value<VALUE> read() {
    return new InMemoryValue<VALUE>(this.get());
  }

  @Override
  public VALUE get() {
    byte[] data = cache.getCurrentData().getData();
    VALUE value;
    try {
      value = serializer.deserialize(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return value;
  }

  @Override
  public void write(Value<VALUE> value) {
    try {
      curator.setData().forPath(path, serializer.serialize(value.get()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
