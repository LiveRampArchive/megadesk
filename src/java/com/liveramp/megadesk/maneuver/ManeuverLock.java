package com.liveramp.megadesk.maneuver;

public interface ManeuverLock {

  public void acquire() throws Exception;

  public void release() throws Exception;

  boolean isAcquiredInThisProcess();
}
