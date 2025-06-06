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
package com.tc.objectserver.entity;

import com.tc.async.api.Sink;
import com.tc.classloader.ServiceLocator;
import com.tc.entity.VoltronEntityMessage;
import com.tc.exception.ServerException;
import com.tc.exception.TCShutdownServerException;
import com.tc.net.ClientID;
import com.tc.object.ClientInstanceID;
import com.tc.object.EntityDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityServerService;

import com.tc.object.EntityID;
import com.tc.object.FetchID;
import com.tc.objectserver.api.EntityManager;
import com.tc.objectserver.api.ManagedEntity;
import com.tc.services.TerracottaServiceProviderRegistry;
import com.tc.util.Assert;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodec;
import com.tc.objectserver.api.ManagementKeyCallback;
import com.tc.objectserver.core.impl.ManagementTopologyEventCollector;
import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.function.Predicate;


public class EntityManagerImpl implements EntityManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityManagerImpl.class);
  private final ConcurrentMap<EntityID, FetchID> entities = new ConcurrentHashMap<>();
  private final ConcurrentMap<FetchID, ManagedEntity> entityIndex = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, EntityServerService<EntityMessage, EntityResponse>> entityServices = new ConcurrentHashMap<>();

  private final ServerEntityFactory creationLoader;
  private final TerracottaServiceProviderRegistry serviceRegistry;
  private final ClientEntityStateManager clientEntityStateManager;
  private final ManagementTopologyEventCollector eventCollector;
  
  private final ManagementKeyCallback flushLocalPipeline;
  private Sink<VoltronEntityMessage> messageSelf;
  
  private final RequestProcessor processorPipeline;
  private boolean shouldCreateActiveEntities;
  
  private final Semaphore snapshotLock = new Semaphore(1); // sync and create or destroy are mutually exclusive
  
  // The sort comparator.
  private final Comparator<ManagedEntity> consumerIdSorter = new Comparator<ManagedEntity>() {
    @Override
    public int compare(ManagedEntity o1, ManagedEntity o2) {
      long firstID = o1.getConsumerID();
      long secondID = o2.getConsumerID();
      // NOTE:  The ids are unique.
      Assert.assertTrue(firstID != secondID);
      return (firstID > secondID)
          ? 1
          : -1;
    }};


  public EntityManagerImpl(TerracottaServiceProviderRegistry serviceRegistry, 
      ClientEntityStateManager clientEntityStateManager, ManagementTopologyEventCollector eventCollector, 
      RequestProcessor processor, ManagementKeyCallback flushLocalPipeline, ServiceLocator locator) {
    this.serviceRegistry = serviceRegistry;
    this.clientEntityStateManager = clientEntityStateManager;
    this.eventCollector = eventCollector;
    this.processorPipeline = processor;
    // By default, the server starts up in a passive mode so we will create passive entities.
    this.shouldCreateActiveEntities = false;
    this.creationLoader = new ServerEntityFactory(locator);
    this.flushLocalPipeline = flushLocalPipeline;
    ManagedEntity platform = createPlatformEntity();
    entities.put(platform.getID(), PlatformEntity.PLATFORM_FETCH_ID);
    entityIndex.put(PlatformEntity.PLATFORM_FETCH_ID, platform);
  }
  
  public void setMessageSink(Sink<VoltronEntityMessage> sink) {
    this.messageSelf = sink;
  }

  private ManagedEntity createPlatformEntity() {
    return new PlatformEntity(messageSelf, processorPipeline);
  }

  @Override
  public ServerEntityFactory getEntityLoader() {
    return this.creationLoader;
  }

  @Override
  public boolean canDelete(EntityID entityID) {
    return !creationLoader.isPermanentEntity(entityID);
  }

  public void shutdown() {
    for (EntityServerService<?,?> service : entityServices.values()) {
      try {
        if (service instanceof Closeable) {
          ((Closeable)service).close();
        }
        if (service instanceof AutoCloseable) {
          ((AutoCloseable)service).close();
        }
      } catch (Exception e) {
        LOGGER.warn("error closing entity service", e);
      }
    }
  }

  @Override
  public List<VoltronEntityMessage> enterActiveState() {
    // We can't enter active twice.
    Assert.assertFalse(this.shouldCreateActiveEntities);
    //  locking the snapshotting until full active is achieved 
    snapshotLock.acquireUninterruptibly();
    try {
      // Tell our implementation-provided services to become active (since they might have modes of operation).
      this.serviceRegistry.notifyServerDidBecomeActive();

      // Set the state of the manager.
      this.shouldCreateActiveEntities = true;
      // We can promote directly because this method is only called from PTH initialize 
      //  thus, this only happens once RTH is spun down and PTH is beginning to spin up.  We know the request queues are clear
      // issue-439: We need to sort these entities, ascending by consumerID.
      List<ManagedEntity> sortingList = new ArrayList<ManagedEntity>(this.entityIndex.values());
      List<VoltronEntityMessage> reconnectDone = new ArrayList<>(this.entityIndex.size());
      Collections.sort(sortingList, this.consumerIdSorter);
      for (ManagedEntity entity : sortingList) {
        try {
            reconnectDone.add(new LocalPipelineFlushMessage(
              EntityDescriptor.createDescriptorForInvoke(new FetchID(entity.getConsumerID()), ClientInstanceID.NULL_ID), 
              entity.promoteEntity()));
        } catch (ConfigurationException ce) {
          String errMsg = "failure to promote entity: " + entity.getID();
          LOGGER.error(errMsg, ce);
          throw new TCShutdownServerException(errMsg, ce);
        }
      }
  //  only enter active state after all the entities have promoted to active
      processorPipeline.enterActiveState();
      return reconnectDone;
    } finally {
      snapshotLock.release();
    }
  }

  @Override
  public ManagedEntity createEntity(EntityID id, long version, long consumerID) throws ServerException {
    // Valid entity versions start at 1.
    EntityServerService service = getVersionCheckedService(id, version);
    snapshotLock.acquireUninterruptibly();
    try {
    //  if active, reuse the managed entity if it is mapped to an id.  if passive, MUST map the id to the index of the managed entity
      FetchID current = entities.compute(id, (eid, fetch)-> shouldCreateActiveEntities ? Optional.ofNullable(fetch).orElse(new FetchID(consumerID)) : new FetchID(consumerID));
      ManagedEntity temp = entityIndex.computeIfAbsent(current, (fetch)->
        new ManagedEntityImpl(id, version, consumerID, flushLocalPipeline, serviceRegistry.subRegistry(consumerID),
          clientEntityStateManager, eventCollector, this.messageSelf, processorPipeline, service, shouldCreateActiveEntities, canDelete(id)));

      return temp;
    } finally {
      snapshotLock.release();
    }
  }

  @Override
  public void loadExisting(EntityID entityID, long recordedVersion, long consumerID, boolean canDelete, byte[] configuration) throws ServerException {
    // Valid entity versions start at 1.
    Assert.assertTrue(recordedVersion > 0);
    EntityServerService service = getVersionCheckedService(entityID, recordedVersion);
    FetchID set = new FetchID(consumerID);
    Object checkNull = entities.put(entityID, set);
    Assert.assertNull(checkNull); //  must be null, nothing should be competing
    ManagedEntity temp = new ManagedEntityImpl(entityID, recordedVersion, consumerID, flushLocalPipeline, 
          serviceRegistry.subRegistry(consumerID), clientEntityStateManager, this.eventCollector, this.messageSelf,
          processorPipeline, service, this.shouldCreateActiveEntities, canDelete);
    
    checkNull = entityIndex.put(set, temp);
    Assert.assertNull(checkNull); //  must be null, nothing should be competing
    try {
      temp.loadEntity(configuration);
    } catch (ConfigurationException ce) {
      String errMsg = "failure to load an existing entity: " + entityID;
      LOGGER.error(errMsg, ce);
      throw new TCShutdownServerException(errMsg, ce);
    }
  }

  @Override
  public boolean removeDestroyed(FetchID id) {
    snapshotLock.acquireUninterruptibly();
    try {
      ManagedEntity e = entityIndex.computeIfPresent(id, (fetch,entity)->{
        if (entity.isRemoveable()) {
          entities.entrySet().removeIf(i->i.getValue().equals(fetch));
          return null;
        } else {
          return entity;
        }
      });
      
      if (e == null) {
        LOGGER.debug("removed " + id);
        return true;
      } else {
        return false;
      }
    } finally {
      snapshotLock.release();
    }
  }

  @Override
  public Optional<ManagedEntity> getEntity(EntityDescriptor descriptor) throws ServerException {
    if (descriptor.isIndexed()) {
      return getEntity(descriptor.getFetchID());
    } else {
      return getEntity(descriptor.getEntityID(), descriptor.getClientSideVersion());
    }
  }
  
  private Optional<ManagedEntity> getEntity(FetchID idx) {
    Assert.assertFalse(idx.isNull());
    return Optional.ofNullable(this.entityIndex.get(idx));
  }
  
  private Optional<ManagedEntity> getEntity(EntityID id, long version) throws ServerException {
    Assert.assertNotNull(id);
    if (EntityID.NULL_ID == id) {
//  just do instance check, believe it or not, equality check is expensive due to frequency called
//  short circuit for null entity, it's never here
      return Optional.empty();
    }
    FetchID fetch = entities.get(id);
    if (fetch != null) {
      ManagedEntity entity = entityIndex.get(fetch);
      //  if the version in the descriptor is not valid, don't check 
      //  check the provided version against the version of the entity
      if (version > 0 && entity.getVersion() != version) {
        throw ServerException.createEntityVersionMismatch(id, entity.getVersion() + " does not match " + version);
      }
      return Optional.of(entity);
    } else {
      return getCompatibleEntity(id, version);
    }
  }

  private Optional<ManagedEntity> getCompatibleEntity(EntityID id, long version) throws ServerException {
    for (Map.Entry<FetchID, ManagedEntity> e : entityIndex.entrySet()) {
      if (e.getValue().isCompatibleEntity(id)) {
        entities.put(id, e.getKey());
        if (version > 0 && e.getValue().getVersion() != version) {
          throw ServerException.createEntityVersionMismatch(id, e.getValue().getVersion() + " does not match " + version);
        }
        return Optional.of(e.getValue());
      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<ManagedEntity> getAll() {
    return new ArrayList<>(entityIndex.values());
  }
  
  @Override
  public List<ManagedEntity> snapshot(Predicate<ManagedEntity> runFirst) {
    snapshotLock.acquireUninterruptibly();
    List<ManagedEntity> sortingList = new ArrayList<>(this.entityIndex.values());
    try {
      Collections.sort(sortingList, this.consumerIdSorter);
      if (runFirst != null) {
        Iterator<ManagedEntity> list = sortingList.iterator();
        while (list.hasNext()) {
          if (!runFirst.test(list.next())) {
            list.remove();
          }
        }
      }
      return sortingList;
    } finally {
      snapshotLock.release();
    }
  }

  private EntityServerService<EntityMessage, EntityResponse> getVersionCheckedService(EntityID entityID, long version) throws ServerException {
    // Valid entity versions start at 1.
    String typeName = entityID.getClassName();
    EntityServerService<EntityMessage, EntityResponse> service = entityServices.get(typeName);
    if (service == null) {
      try {
        service = (EntityServerService)this.creationLoader.getService(typeName);
      } catch (ClassNotFoundException notfound) {
        throw ServerException.createEntityNotProvided(entityID);
      }
      // getService only fails to resolve by throwing.
      Assert.assertNotNull(service);
      Object oldService = entityServices.putIfAbsent(typeName, service);
      // This needs to be null or else there was some kind of unexpected concurrent access which would have caused failure or a duplicate entry.
      Assert.assertNull(oldService);
    }
    
    // We must have a service by now or we would have thrown.
    Assert.assertNotNull(service);
    long serviceVersion = service.getVersion();
    if (version > 0 && serviceVersion != version) {
        throw ServerException.createEntityVersionMismatch(entityID, serviceVersion + " does not match " + version);
    }
    return service;
  }
  
  @Override
  public Set<ClientID> resetReferences() {
    for (ManagedEntity me : entityIndex.values()) {
      me.resetReferences(0);
    }
    return clientEntityStateManager.clearClientReferences();
  }

  @Override
  public MessageCodec<? extends EntityMessage, ? extends EntityResponse> getMessageCodec(EntityDescriptor eid) {
    ManagedEntity e = this.entityIndex.get(eid.getFetchID());
    if (e != null) {
      return e.getCodec();
    }
    return null;
  }

  @Override
  public String toString() {
    return "EntityManagerImpl{" + "entities=" + entities.keySet() + '}';
  }

  @Override
  public Map<String, ?> getStateMap() {
    Map<String, Object> entityMap = new LinkedHashMap<>();
    Set<Map.Entry<EntityID, FetchID>> entries = entities.entrySet();
    entityMap.put("className", this.getClass().getName());
    entityMap.put("size", entries.size());
    List<Map<String, Object>> entities  = new ArrayList<>(entityIndex.size());
    entityMap.put("entities", entities);
    entityIndex.values().forEach(entity->entities.add(entity.getState()));
    return entityMap;
  }
}

