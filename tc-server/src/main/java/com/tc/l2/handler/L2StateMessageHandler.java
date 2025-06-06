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
package com.tc.l2.handler;

import com.tc.async.api.AbstractEventHandler;
import com.tc.async.api.ConfigurationContext;
import com.tc.l2.msg.L2StateMessage;
import com.tc.l2.state.StateManager;
import com.tc.objectserver.core.api.ServerConfigurationContext;

public class L2StateMessageHandler extends AbstractEventHandler<L2StateMessage> {

  private StateManager stateManager;

  @Override
  public void handleEvent(L2StateMessage clusterMsg) {
    stateManager.handleClusterStateMessage(clusterMsg);
  }
  
  @Override
  public void initialize(ConfigurationContext context) {
    super.initialize(context);
    ServerConfigurationContext oscc = (ServerConfigurationContext) context;
    this.stateManager = oscc.getL2Coordinator().getStateManager();
  }

}
