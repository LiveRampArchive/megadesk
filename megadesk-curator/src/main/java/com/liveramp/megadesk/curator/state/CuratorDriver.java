package com.liveramp.megadesk.curator.state;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.lib.BaseDriver;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

public class CuratorDriver<VALUE> {

  public static <VALUE> Driver<VALUE> build(
      String path,
      CuratorFramework framework,
      SerializationHandler<VALUE> serializer) {

    ReadWriteLock executionLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, path + "/executionLock"));
    ReadWriteLock persistenceLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, path + "/persistenceLock"));
    Persistence<VALUE> persistence = new CuratorPersistence<VALUE>(framework, path, serializer);
    Reference<VALUE> reference = new CuratorReference<VALUE>(path);

    return new BaseDriver<VALUE>(reference, persistence, persistenceLock, executionLock);
  }
}
