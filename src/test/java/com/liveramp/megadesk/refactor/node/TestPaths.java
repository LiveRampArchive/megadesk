/**
 *  Copyright 2014 LiveRamp
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

package com.liveramp.megadesk.refactor.node;

import java.util.Arrays;

import com.liveramp.megadesk.test.BaseTestCase;

public class TestPaths extends BaseTestCase {

  public void testMain() {
    assertEquals("/a/b/c", Paths.append("a", "b", "c"));
    assertEquals("/a/b/c", Paths.append("/a", "/b", "/c"));
    assertEquals("/a", Paths.append("a"));

    assertEquals("/a/b/c", Paths.sanitize("//a//b/c"));
    assertEquals("/", Paths.sanitize("////"));

    assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(Paths.split("/a/b/c")));
    assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(Paths.split("//a//b//c")));
    assertEquals(Arrays.asList("a"), Arrays.asList(Paths.split("a")));

    assertEquals("/a/b", Paths.parent("//a//b//c"));
    assertEquals("/", Paths.parent("/a"));
    assertEquals("/", Paths.parent("//"));
  }
}
