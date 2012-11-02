package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;

import java.util.List;

public interface Maneuver {

  public String getId();

  public List<Read> getReads();

  public List<Device> getWrites();

  public void acquire() throws Exception;

  public void release() throws Exception;

  public <T> void write(Device<T> device, T state) throws Exception;
}
