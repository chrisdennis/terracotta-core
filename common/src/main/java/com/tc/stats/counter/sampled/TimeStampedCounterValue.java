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
package com.tc.stats.counter.sampled;

import java.io.Serializable;

/**
 * A counter value at a particular time instance
 */
public class TimeStampedCounterValue implements Serializable {
  private final long counterValue;
  private final long timestamp;

  public TimeStampedCounterValue(long timestamp, long value) {
    this.timestamp = timestamp;
    this.counterValue = value;
  }

  public long getCounterValue() {
    return this.counterValue;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return "value: " + this.counterValue + ", timestamp: " + this.timestamp;
  }

}
