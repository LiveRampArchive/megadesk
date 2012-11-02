package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.Read;

import java.util.List;

public interface Maneuver<T, SELF extends Maneuver> {

  public String getId();

  public List<Read> getReads();

  public List<Resource> getWrites();

  public SELF reads(Read... reads);

  public SELF writes(Resource... writes);

  public void acquire() throws Exception;

  public void release() throws Exception;

  public T getData() throws Exception;

  public void setData(T data) throws Exception;

  public void write(Resource resource, Object data) throws Exception;
}
