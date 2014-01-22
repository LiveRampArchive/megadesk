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

  public static <VALUE> Driver<VALUE> build(String name, SerializationHandler<VALUE> serializer) {

    CuratorFramework framework = CuratorConfiguration.INSTANCE.getFramework();
    String basePath = CuratorConfiguration.INSTANCE.getPathMaker().makePath(name);

    ReadWriteLock executionLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, basePath + "/executionLock"));
    ReadWriteLock persistenceLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, basePath + "/persistenceLock"));
    Persistence<VALUE> persistence = new CuratorPersistence<VALUE>(framework, basePath, serializer);
    Reference<VALUE> reference = new CuratorReference<VALUE>(name);

    return new BaseDriver<VALUE>(reference, persistence, persistenceLock, executionLock);
  }
}
