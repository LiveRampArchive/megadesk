package com.liveramp.megadesk.curator;

import com.netflix.curator.framework.CuratorFramework;

public class TimeDevice extends CuratorDevice<Long> {

  public TimeDevice(CuratorFramework curator, String id) throws Exception {
    super(curator, id, null);
  }

  @Override
  public Long getData() {
    return System.currentTimeMillis();
  }

  @Override
  public void setData(Long data) throws Exception {
    throw new IllegalAccessException("TimeDevice data cannot be set.");
  }

  @Override
  public void setData(String owner, Long data) throws Exception {
    throw new IllegalAccessException("TimeDevice data cannot be set.");
  }
}
