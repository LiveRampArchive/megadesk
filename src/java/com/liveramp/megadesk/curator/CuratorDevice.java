package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.device.BaseDevice;
import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.DeviceReadLock;
import com.liveramp.megadesk.device.DeviceWriteLock;
import com.liveramp.megadesk.status.StatusSerialization;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

public class CuratorDevice<T> extends BaseDevice<T> implements Device<T> {

  private static final Logger LOGGER = Logger.getLogger(CuratorDevice.class);

  private static final String DEVICES_PATH = "/devices";

  private final CuratorFramework curator;
  private final String path;
  private final StatusSerialization<T> statusSerialization;
  private final CuratorDeviceLock<T> lock;

  public CuratorDevice(CuratorFramework curator,
                       String id,
                       StatusSerialization<T> statusSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.statusSerialization = statusSerialization;
    this.path = ZkPath.append(DEVICES_PATH, id);
    this.lock = new CuratorDeviceLock<T>(curator, this);
  }

  @Override
  public T doGetStatus() throws Exception {
    byte[] payload = curator.getData().forPath(path);
    return statusSerialization.deserialize(payload);
  }

  @Override
  protected void doSetStatus(T status) throws Exception {
    LOGGER.info("Setting device '" + getId() + "' to status '" + status + "'");
    curator.setData().forPath(path, statusSerialization.serialize(status));
  }

  @Override
  public DeviceReadLock getReadLock() {
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
