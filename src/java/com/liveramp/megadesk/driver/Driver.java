package com.liveramp.megadesk.driver;

import com.liveramp.megadesk.resource.ResourcesLock;

public interface Driver {

  public ResourcesLock getResourcesLock();
}
