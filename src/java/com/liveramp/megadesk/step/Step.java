/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.megadesk.step;

import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.dependency.DependencyWatcher;
import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;

import java.util.List;

public interface Step<T, SELF extends Step> {

  public String getId();

  public List<Read> getReads();

  public List<Resource> getWrites();

  public SELF reads(Read... reads);

  public SELF reads(Resource resource, Dependency dependency);

  public SELF writes(Resource... writes);

  public void acquire() throws Exception;

  public void release() throws Exception;

  public T get() throws Exception;

  public T get(DependencyWatcher watcher) throws Exception;

  public void set(T data) throws Exception;

  public void write(Resource resource, Object data) throws Exception;
}
