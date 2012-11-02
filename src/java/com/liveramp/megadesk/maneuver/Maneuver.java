package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;

import java.util.List;

public interface Maneuver {

  public String getId();

  public List<Read> getReads();

  public List<Resource> getWrites();

  public void acquire() throws Exception;

  public void release() throws Exception;

  public <T> void set(Resource<T> resource, T state) throws Exception;
}
