package com.liveramp.megadesk.state.check;

import com.liveramp.megadesk.resource.Resource;

public interface StateCheck<T> {

  public boolean check(Resource<T> resource) throws Exception;
}
