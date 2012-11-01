package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.BaseResource;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.state.StateSerialization;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

public class CuratorResource<T> extends BaseResource<T> implements Resource<T> {

  private static final Logger LOGGER = Logger.getLogger(CuratorResource.class);

  private static final String RESOURCES_PATH = "/resources";

  private final CuratorFramework curator;
  private final String path;
  private final StateSerialization<T> stateSerialization;
  private final CuratorResourceLock<T> lock;

  public CuratorResource(CuratorFramework curator,
                         String id,
                         StateSerialization<T> stateSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.stateSerialization = stateSerialization;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock<T>(curator, this);
  }

  @Override
  public T doGetState() throws Exception {
    byte[] payload = curator.getData().forPath(path);
    return stateSerialization.deserialize(payload);
  }

  @Override
  protected void doSetState(T state) throws Exception {
    LOGGER.info("Setting resource '" + getId() + "' to state '" + state + "'");
    curator.setData().forPath(path, stateSerialization.serialize(state));
  }

  @Override
  public ResourceReadLock getReadLock() {
    return lock.getReadLock();
  }

  @Override
  public ResourceWriteLock getWriteLock() {
    return lock.getWriteLock();
  }

  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    try {
      return "[CuratorResource '" + getId()
          + "', writer: '" + getWriteLock().getOwner()
          + "', readers: " + getReadLock().getOwners() + "]";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
