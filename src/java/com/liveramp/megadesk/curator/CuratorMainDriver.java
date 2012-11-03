package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.driver.MainDriver;
import com.liveramp.megadesk.resource.ResourcesLock;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorMainDriver implements MainDriver {

  private static final String RESOURCES_LOCK_PATH = "/resources-lock";

  private final ResourcesLock resourcesLock;

  public CuratorMainDriver(CuratorFramework curator) {
    this.resourcesLock = new CuratorResourcesLock(curator, RESOURCES_LOCK_PATH);
  }

  @Override
  public ResourcesLock getResourcesLock() {
    return resourcesLock;
  }
}
