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
package com.tc.net.protocol.transport;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesImpl;
import com.tc.properties.TCPropertiesConsts;

import static com.tc.net.protocol.transport.ConnectionHealthCheckerImpl.HealthCheckerMonitorThreadEngine;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Eugene Shelestovich
 */
public class HealthCheckerMonitorThreadEngineTest {

  private static final Logger logger = LoggerFactory.getLogger(HealthCheckerMonitorThreadEngineTest.class);

  @Test
  public void testAllowCheckTimeIfEnabledInConfig() {
    final TCProperties props = TCPropertiesImpl.getProperties().getPropertiesFor(TCPropertiesConsts.L2_L2_HEALTH_CHECK_CATEGORY);
    // enable time checking
    props.setProperty("checkTime.enabled", "true");
    // ignore interval
    props.setProperty("checkTime.interval", "-1");
    HealthCheckerConfigImpl config = new HealthCheckerConfigImpl(props, "test-config");
    final HealthCheckerMonitorThreadEngine engine = new HealthCheckerMonitorThreadEngine(config, null, ()->Boolean.TRUE, logger);

    assertTrue(engine.canCheckTime());
  }

  @Test
  public void testDisallowCheckTimeIfDisabledInConfig() {
    final TCProperties props = TCPropertiesImpl.getProperties().getPropertiesFor(TCPropertiesConsts.L2_L2_HEALTH_CHECK_CATEGORY);
    // disable time checking
    props.setProperty("checkTime.enabled", "false");
    HealthCheckerConfigImpl config = new HealthCheckerConfigImpl(props, "test-config");
    final HealthCheckerMonitorThreadEngine engine = new HealthCheckerMonitorThreadEngine(config, null, ()->Boolean.TRUE, logger);

    assertFalse(engine.canCheckTime());
  }

  @Test
  public void testDisallowCheckTimeIfIntervalNotExceeded() {
    final TCProperties props = TCPropertiesImpl.getProperties().getPropertiesFor(TCPropertiesConsts.L2_L2_HEALTH_CHECK_CATEGORY);
    // set short interval
    props.setProperty("checkTime.interval", "900000");
    HealthCheckerConfigImpl config = new HealthCheckerConfigImpl(props, "test-config");
    final HealthCheckerMonitorThreadEngine engine = new HealthCheckerMonitorThreadEngine(config, null, ()->Boolean.TRUE, logger);

    assertFalse(engine.canCheckTime());
  }

}
