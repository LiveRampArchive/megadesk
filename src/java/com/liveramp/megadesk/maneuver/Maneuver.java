package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;

import java.util.List;

public interface Maneuver<T, CRTP extends Maneuver> {

  public String getId();

  public List<Read> getReads();

  public List<Device> getWrites();

  public CRTP reads(Read... reads);

  public CRTP writes(Device... writes);

  public void acquire() throws Exception;

  public void release() throws Exception;

  public T getStatus() throws Exception;

  public void setStatus(T status) throws Exception;

  public void write(Device device, Object status) throws Exception;
}
