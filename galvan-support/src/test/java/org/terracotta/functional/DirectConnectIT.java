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
package org.terracotta.functional;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.Diagnostics;
import org.terracotta.connection.DiagnosticsFactory;
import org.terracotta.testing.rules.BasicExternalClusterBuilder;
import org.terracotta.testing.rules.Cluster;

/**
 *
 */
public class DirectConnectIT {

  Logger LOGGER = LoggerFactory.getLogger(DirectConnectIT.class);
  
  @ClassRule
  public static final Cluster CLUSTER = BasicExternalClusterBuilder.newCluster(2)
          .withFailoverPriorityVoterCount(0)
//          .withSystemProperty("logback.debug", "true")
          .withClientReconnectWindowTime(30)
      .build();

  @Test
  public void testPassiveCrash() throws Exception {
    LOGGER.info("starting test");
    String[] clusterHostPorts = CLUSTER.getClusterHostPorts();
    CLUSTER.expectCrashes(true);
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();
    Properties prop = new Properties();
    prop.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, "-1");
    for (String hostPort: clusterHostPorts) {
      String[] hp = hostPort.split("[:]");
      InetSocketAddress inet = InetSocketAddress.createUnresolved(hp[0], Integer.parseInt(hp[1]));
      try (Diagnostics d = DiagnosticsFactory.connect(inet, prop)) {
        System.out.println(d.getState());
      } catch (Exception to) {
        to.printStackTrace();
      }

    }
  }

}
