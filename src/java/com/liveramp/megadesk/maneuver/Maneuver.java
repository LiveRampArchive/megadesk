package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;

import java.util.List;

public interface Maneuver {

  public String getId();

  public List<Read> getReads();

  public List<Device> getWrites();

  public Maneuver reads(Read... reads);

  public Maneuver writes(Device... writes);

  public void acquire() throws Exception;

  public void release() throws Exception;

  public <T> void write(Device<T> device, T state) throws Exception;
}
