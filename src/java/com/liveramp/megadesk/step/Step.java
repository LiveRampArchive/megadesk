package com.liveramp.megadesk.step;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;

import java.util.List;

public interface Step {

  public String getId();

  public List<Read> getReads();

  public List<Resource> getWrites();

  public void attempt() throws Exception;

  public void complete() throws Exception;

  public void setState(Resource resource, String state) throws Exception;
}