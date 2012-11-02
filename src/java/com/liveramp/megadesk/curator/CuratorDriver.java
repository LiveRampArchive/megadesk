package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.driver.Driver;
import com.liveramp.megadesk.resource.ResourcesLock;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorDriver implements Driver {

  private static final String RESOURCES_LOCK_PATH = "/resources-lock";

  private final ResourcesLock resourcesLock;

  public CuratorDriver(CuratorFramework curator) {
    this.resourcesLock = new CuratorResourcesLock(curator, RESOURCES_LOCK_PATH);
  }

  @Override
  public ResourcesLock getResourcesLock() {
    return resourcesLock;
  }
}
