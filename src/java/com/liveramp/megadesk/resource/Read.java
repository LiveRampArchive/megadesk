package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.data.DataCheck;

public class Read<T> {

  private final Resource<T> resource;
  private final DataCheck<T> dataCheck;

  public Read(Resource<T> resource, DataCheck<T> dataCheck) {
    this.resource = resource;
    this.dataCheck = dataCheck;
  }

  public Resource<T> getResource() {
    return resource;
  }

  public DataCheck<T> getDataCheck() {
    return dataCheck;
  }
}
