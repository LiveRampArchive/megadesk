package com.liveramp.megadesk;

import com.liveramp.megadesk.driver.MainDriver;
import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.driver.StepDriver;

public interface Megadesk {

  public MainDriver getMainDriver();

  public ResourceDriver getResourceDriver(String id) throws Exception;

  public StepDriver getStepDriver(String id) throws Exception;
}
