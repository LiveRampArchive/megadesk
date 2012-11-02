package com.liveramp.megadesk.curator;

import com.netflix.curator.framework.CuratorFramework;

public class TimeResource extends CuratorResource<Long> {

  public TimeResource(CuratorFramework curator, String id) throws Exception {
    super(curator, id, null);
  }

  @Override
  public Long getData() {
    return System.currentTimeMillis();
  }

  @Override
  public void setData(Long data) throws Exception {
    throw new IllegalAccessException("TimeResource data cannot be set.");
  }

  @Override
  public void setData(String owner, Long data) throws Exception {
    throw new IllegalAccessException("TimeResource data cannot be set.");
  }
}
