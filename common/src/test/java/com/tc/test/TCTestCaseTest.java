/*
 *  Copyright Terracotta, Inc.
 *  Copyright IBM Corp. 2024, 2025
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
 *
 */
package com.tc.test;

import com.tc.util.runtime.Vm;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import org.junit.Ignore;

@Ignore
public class TCTestCaseTest extends TCTestCase {

  public TCTestCaseTest() {
    if (Vm.isIBM()) {
      disableTest();
    }
  }

  public void testHeapDump() throws IOException {
    assertEquals(0, getHprofs().length);
    dumpHeap(getTempDirectory());
    assertEquals(1, getHprofs().length);
    dumpHeap(getTempDirectory());
    assertEquals(2, getHprofs().length);
    dumpHeap(getTempDirectory());
  }

  private File[] getHprofs() throws IOException {
    return getTempDirectory().listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".hprof");
      }
    });
  }

}
