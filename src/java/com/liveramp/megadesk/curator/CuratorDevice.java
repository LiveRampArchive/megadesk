package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.device.BaseDevice;
import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.DeviceReadLock;
import com.liveramp.megadesk.device.DeviceWriteLock;
import com.liveramp.megadesk.serialization.Serialization;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

public class CuratorDevice<T> extends BaseDevice<T> implements Device<T> {

  private static final Logger LOGGER = Logger.getLogger(CuratorDevice.class);

  private static final String DEVICES_PATH = "/devices";

  private final CuratorFramework curator;
  private final String path;
  private final Serialization<T> dataSerialization;
  private final CuratorDeviceLock<T> lock;

  public CuratorDevice(CuratorFramework curator,
                       String id,
                       Serialization<T> dataSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.dataSerialization = dataSerialization;
    this.path = ZkPath.append(DEVICES_PATH, id);
    this.lock = new CuratorDeviceLock<T>(curator, this);
  }

  @Override
  public T doGetData() throws Exception {
    byte[] payload = curator.getData().forPath(path);
    return dataSerialization.deserialize(payload);
  }

  @Override
  protected void doSetData(T data) throws Exception {
    LOGGER.info("Setting device '" + getId() + "' to data '" + data + "'");
    curator.setData().forPath(path, dataSerialization.serialize(data));
  }

  @Override
  public DeviceReadLock<T> getReadLock() {
    return lock.getReadLock();
  }

  @Override
  public DeviceWriteLock getWriteLock() {
    return lock.getWriteLock();
  }

  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    try {
      return "[CuratorDevice '" + getId()
          + "', writer: '" + getWriteLock().getOwner()
          + "', readers: " + getReadLock().getOwners() + "]";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
