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
package com.tc.objectserver.storage.api;

/**
 * Additional Offheap Stats which can be accessed vis JMX beans. These methods are not exposed in OffheapStats as they
 * can affect performance and don't want users like dev-consoles to invoke them frequently.
 */
public interface OffheapJMXStats extends OffheapStats {

  public static final OffheapJMXStats NULL_OFFHEAP_JMXSTATS = new OffheapJMXStats() {

                                                              @Override
                                                              public long getOffheapReservedSize() {
                                                                return 0;
                                                              }

                                                              @Override
                                                              public long getOffheapUsedSize() {
                                                                return 0;
                                                              }

                                                              @Override
                                                              public long getOffheapMaxSize() {
                                                                return 0;
                                                              }

                                                              
                                                            };
}
