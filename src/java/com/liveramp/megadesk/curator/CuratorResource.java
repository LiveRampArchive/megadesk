package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

public class CuratorResource implements Resource {

  private static final Logger LOGGER = Logger.getLogger(CuratorResource.class);

  private static final String RESOURCES_PATH = "/resources";

  private final CuratorFramework curator;
  private final String id;
  private final String path;
  private final CuratorResourceLock lock;

  public CuratorResource(CuratorFramework curator, String id) throws Exception {
    this.curator = curator;
    this.id = id;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock(curator, this);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void setState(Step step, String state) throws Exception {
    LOGGER.info("Setting resource " + getId() + " state to " + state);
    curator.setData().forPath(path, state.getBytes());
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
  public String getState() throws Exception {
    byte[] payload = curator.getData().forPath(path);
    return (payload == null || payload.length == 0) ? null : new String(payload);
  }
}
