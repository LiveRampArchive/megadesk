package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.BaseResource;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.serialization.Serialization;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

public class CuratorResource<T> extends BaseResource<T> implements Resource<T> {

  private static final Logger LOGGER = Logger.getLogger(CuratorResource.class);

  private static final String RESOURCES_PATH = "/megadesk/resources";

  private final CuratorFramework curator;
  private final String path;
  private final Serialization<T> dataSerialization;
  private final CuratorResourceLock<T> lock;

  public CuratorResource(CuratorFramework curator,
                         String id,
                         Serialization<T> dataSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.dataSerialization = dataSerialization;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock<T>(curator, this);
  }

  @Override
  public T doGetData() throws Exception {
    byte[] payload = curator.getData().forPath(path);
    return dataSerialization.deserialize(payload);
  }

  @Override
  protected void doSetData(T data) throws Exception {
    LOGGER.info("Setting resource '" + getId() + "' to data '" + data + "'");
    curator.setData().forPath(path, dataSerialization.serialize(data));
  }

  @Override
  public ResourceReadLock<T> getReadLock() {
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
