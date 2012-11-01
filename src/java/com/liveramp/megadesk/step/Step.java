package com.liveramp.megadesk.step;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;

import java.util.List;

public interface Step {

  public String getId();

  public List<Read> getReads();

  public List<Resource> getWrites();

  public void acquire() throws Exception;

  public void release() throws Exception;

  public <T> void setState(Resource<T> resource, T state) throws Exception;
}
