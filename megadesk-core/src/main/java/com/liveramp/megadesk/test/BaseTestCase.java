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

package com.liveramp.megadesk.test;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.TTCCLayout;
import org.junit.Before;

public abstract class BaseTestCase {

  static {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(new TTCCLayout(), ConsoleAppender.SYSTEM_ERR));
  }

  @Before
  public void printTestStart() throws Exception {
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }
}
