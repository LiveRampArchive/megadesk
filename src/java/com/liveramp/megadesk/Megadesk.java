package com.liveramp.megadesk;

import com.liveramp.megadesk.driver.Driver;
import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.driver.StepDriver;

public interface Megadesk {

  public Driver getDriver();

  public ResourceDriver getResourceDriver(String id) throws Exception;

  public StepDriver getStepDriver(String id) throws Exception;
}
