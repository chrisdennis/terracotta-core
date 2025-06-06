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
package com.tc.objectserver.api;

import com.tc.l2.msg.SyncReplicationActivity;
import java.util.EnumMap;

/**
 * These "actions" represent the superset of "Request.Type" values. That is,
 * they are an internal representation of what was sent over the wire OR derived
 * for some internal purpose. This is to loosen the coupling between
 * ServerEntityRequest and Request.
 */
public enum ServerEntityAction {
  /**
   * An error value used when the action should not be seen.
   */
  INVALID,
  /**
   * Same as Request.Type.
   */
  FETCH_ENTITY{
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  /**
   * Same as Request.Type.
   */
  RELEASE_ENTITY{
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  /**
   * Same as Request.Type.
   */
  CREATE_ENTITY {
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  /**
   * Same as Request.Type.
   */
  DESTROY_ENTITY {
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  /**
   * Same as Request.Type.
   */
  INVOKE_ACTION {
    @Override
    public boolean isReplicated() {
      return true;
    }
  },
  /**
   * Ask an entity to synchronize itself to a passive.
   */
  REQUEST_SYNC_ENTITY {
    @Override
    public boolean isReplicated() {
      return true;
    }
  },
  /**
   * Reload the active entity with the new supplied configuration.
   */
  RECONFIGURE_ENTITY{
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  // ***** Messages specific to received passive synchronization data below this point *****
  /**
   * During sync, create an entity which will later by synced.
   */
  RECEIVE_SYNC_CREATE_ENTITY,
  /**
   * Messages related to the synchronization of a specific entity instance
   * follow.
   */
  RECEIVE_SYNC_ENTITY_START_SYNCING,
  /**
   * Messages related to the synchronization of a specific entity instance are
   * now done.
   */
  RECEIVE_SYNC_ENTITY_END,
  /**
   * Messages related to the synchronization of a specific entity concurrency
   * key follow.
   */
  RECEIVE_SYNC_ENTITY_KEY_START,
  /**
   * Messages related to the synchronization of a specific entity concurrency
   * key are now done.
   */
  RECEIVE_SYNC_ENTITY_KEY_END,
  /**
   * A synchronized state message on a specific concurrency key within a
   * specific entity instance.
   */
  RECEIVE_SYNC_PAYLOAD,
  // ***** END: Messages specific to received passive synchronization data *****
  /**
   * An action which should never be replicated, just used to synchronize on the
   * flush of local executor queues. Scheduled on the UNIVERSAL_KEY
   */
  LOCAL_FLUSH,
  /**
   * An action which should never be replicated, just used to clean up a deleted
   * entity, after a pipeline flush.  Also used by passives before failover to 
   * flush the entities requests through the entire pipeline.  Scheduled on the MANAGEMENT_KEY
   */
  MANAGED_ENTITY_GC,
  /**
   * Never replicated, used to clean up a client when it disconnects from the
   * server and needs to cleanup releases.
   */
  DISCONNECT_CLIENT {
    @Override
    public boolean isLifecycle() {
      return true;
    }
  },
  /**
   * An action which should never be replicated, just used to start a
   * synchronization, after a pipeline flush.
   */
  LOCAL_FLUSH_AND_SYNC,
  /**
   * Failover flush, flush the entity pipeline on failover
   */
  FAILOVER_FLUSH,
  /**
   * Used in message replication: we often don't want to replicate the contents
   * of the message or its intent, just information which might be required to
   * correctly order re-sends, after fail-over.
   */
  ORDER_PLACEHOLDER_ONLY {
    @Override
    public boolean isReplicated() {
      return true;
    }
  };

  public boolean isLifecycle() {
    return false;
  }
  
  public boolean isReplicated() {
    return isLifecycle();
  }
  
  private static EnumMap<ServerEntityAction,SyncReplicationActivity.ActivityType> typeMap = new EnumMap(ServerEntityAction.class);
  static {
    typeMap.put(ServerEntityAction.CREATE_ENTITY, SyncReplicationActivity.ActivityType.CREATE_ENTITY);
    typeMap.put(ServerEntityAction.RECONFIGURE_ENTITY, SyncReplicationActivity.ActivityType.RECONFIGURE_ENTITY);
    typeMap.put(ServerEntityAction.DESTROY_ENTITY, SyncReplicationActivity.ActivityType.DESTROY_ENTITY);
    typeMap.put(ServerEntityAction.FETCH_ENTITY, SyncReplicationActivity.ActivityType.FETCH_ENTITY);
    typeMap.put(ServerEntityAction.INVOKE_ACTION, SyncReplicationActivity.ActivityType.INVOKE_ACTION);
    typeMap.put(ServerEntityAction.ORDER_PLACEHOLDER_ONLY, SyncReplicationActivity.ActivityType.ORDERING_PLACEHOLDER);
    typeMap.put(ServerEntityAction.RELEASE_ENTITY, SyncReplicationActivity.ActivityType.RELEASE_ENTITY);
    typeMap.put(ServerEntityAction.REQUEST_SYNC_ENTITY, SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_BEGIN);
    typeMap.put(ServerEntityAction.DISCONNECT_CLIENT, SyncReplicationActivity.ActivityType.DISCONNECT_CLIENT);
  }
  
  public SyncReplicationActivity.ActivityType replicationType() {
    return typeMap.get(this);
  }
}
