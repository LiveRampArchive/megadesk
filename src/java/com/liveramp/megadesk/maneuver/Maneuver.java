package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;

import java.util.List;

public interface Maneuver<T, SELF extends Maneuver> {

  public String getId();

  public List<Read> getReads();

  public List<Device> getWrites();

  public SELF reads(Read... reads);

  public SELF writes(Device... writes);

  public void acquire() throws Exception;

  public void release() throws Exception;

  public T getData() throws Exception;

  public void setData(T data) throws Exception;

  public void write(Device device, Object data) throws Exception;
}
