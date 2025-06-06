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

import org.slf4j.LoggerFactory;

import com.tc.exception.TCRuntimeException;
import com.tc.logging.ConnectionIdLogger;
import com.tc.net.CommStackMismatchException;
import com.tc.net.MaxConnectionsExceededException;
import com.tc.net.ReconnectionRejectedException;
import com.tc.net.core.TCConnection;
import com.tc.net.core.TCConnectionManager;
import com.tc.net.core.event.TCConnectionEvent;
import com.tc.net.protocol.NetworkLayer;
import com.tc.net.protocol.NetworkStackID;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import com.tc.net.protocol.TCProtocolAdaptor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Client implementation of the transport network layer.
 */
public class ClientMessageTransport extends MessageTransportBase {
  public static final long                  TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT = TCPropertiesImpl
                                                                                   .getProperties()
                                                                                   .getLong(TCPropertiesConsts.TC_TRANSPORT_HANDSHAKE_TIMEOUT,
                                                                                            10000);
  private final TCConnectionManager connectionManager;
  private CompletableFuture<NetworkStackID>                         opener;
  private CompletableFuture<SynAckMessage>                          waitForSynAckResult;
  private final WireProtocolAdaptorFactory  wireProtocolAdaptorFactory;
  private final int                         timeout;
  private final ReconnectionRejectedHandler reconnectionRejectedHandler;

  public ClientMessageTransport(TCConnectionManager clientConnectionEstablisher,
                                TransportHandshakeErrorHandler handshakeErrorHandler,
                                TransportHandshakeMessageFactory messageFactory,
                                WireProtocolAdaptorFactory wireProtocolAdaptorFactory, int timeout) {
    this(clientConnectionEstablisher, handshakeErrorHandler, messageFactory, wireProtocolAdaptorFactory, timeout,
         ReconnectionRejectedHandlerL1.SINGLETON);
  }

  /**
   * Constructor for when you want a transport that isn't connected yet (e.g., in a client). This constructor will
   * create an unopened MessageTransport.
   */
  public ClientMessageTransport(TCConnectionManager connectionManager,
                                TransportHandshakeErrorHandler handshakeErrorHandler,
                                TransportHandshakeMessageFactory messageFactory,
                                WireProtocolAdaptorFactory wireProtocolAdaptorFactory, int timeout,
                                ReconnectionRejectedHandler reconnectionRejectedHandler) {

    super(MessageTransportState.STATE_START, handshakeErrorHandler, messageFactory, LoggerFactory.getLogger(ClientMessageTransport.class));
    this.wireProtocolAdaptorFactory = wireProtocolAdaptorFactory;
    this.connectionManager = connectionManager;
    this.timeout = timeout;
    this.reconnectionRejectedHandler = reconnectionRejectedHandler;
  }

  /**
   * Blocking open. Causes a connection to be made. Will throw exceptions if the connect fails.
   * 
   * @throws TCTimeoutException
   * @throws IOException
   * @throws MaxConnectionsExceededException
   */
  @Override
  public NetworkStackID open(InetSocketAddress serverAddress) throws TCTimeoutException, IOException, MaxConnectionsExceededException,
      CommStackMismatchException {
    // XXX: This extra boolean flag is dumb, but it's here because the close event can show up
    // while the lock on isOpen is held here. That will cause a deadlock because the close event is thrown on the
    // comms thread which means that the handshake messages can't be sent.
    // The state machine here needs to be rationalized.
    Future<NetworkStackID> opening = startOpen();
    if (opening != null) {
      try {
        return opening.get();
      } catch (java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof TCTimeoutException) {
          throw (TCTimeoutException)cause;
        } else if (cause instanceof IOException) {
          throw (IOException)cause;
        } else if (cause instanceof MaxConnectionsExceededException) {
          throw (MaxConnectionsExceededException)cause;
        } else if (cause instanceof CommStackMismatchException) {
          throw (CommStackMismatchException)cause;
        } else {
          throw new IOException(cause);
        }
      } catch (java.lang.InterruptedException e) {
        throw new IOException(e);
      }
    }
    Assert.eval("can't open an already open transport", !this.status.isAlive());
    Assert.eval("can't open an already connected transport", !this.isConnected());
    TCConnection connection = null;
    try {
      connection = connect(serverAddress);
      openConnection(connection);
      NetworkStackID nid = new NetworkStackID(getConnectionID().getChannelID());
      if (connection.isClosed()) {
        throw new IOException("closed");
      } else {
        finishOpen(nid);
      }
      return nid;
    } catch (CommStackMismatchException | IOException | MaxConnectionsExceededException | TCTimeoutException | RuntimeException e) {
      finishOpenWithException(e);
      if (connection != null) {
        connection.close();
      }
      throw e;
    }
  }
  /**
   * Tries to make a connection. This is a blocking call.
   * 
   * @return
   * @throws TCTimeoutException
   * @throws IOException
   * @throws MaxConnectionsExceededException
   */
  TCConnection connect(InetSocketAddress sa) throws TCTimeoutException, IOException {
    TCConnection connection = this.connectionManager.createConnection(getProtocolAdapter());
    if (connection == null) {
      throw new IOException("failed to create a new connection");
    }
    fireTransportConnectAttemptEvent();
    try {
      connection.connect(sa, this.timeout);
    } catch (IOException e) {
      connection.close();
      throw e;
    } catch (TCTimeoutException e) {
      connection.close();
      throw e;
    }
    return connection;
  }
  
  @Override
  public void reset() {
    getLogger().info("Resetting connection " + getConnectionID());
    this.disconnect();
    clearConnection();
    clearConnectionID();
  }

  private void handleHandshakeError(HandshakeResult result) throws TransportHandshakeException, MaxConnectionsExceededException,
      CommStackMismatchException, ReconnectionRejectedException {
    if (result.hasErrorContext()) {
      switch (result.getError()) {
        case ERROR_NO_ACTIVE:
          if (this.getProductID().isRedirectEnabled()) {
            throw new NoActiveException();
          } else {
            Assert.assertTrue(getProductID().isInternal());
          }
          break;
        case ERROR_MAX_CONNECTION_EXCEED:
          cleanConnectionWithoutNotifyListeners();
          throw new MaxConnectionsExceededException(getMaxConnectionsExceededMessage(result.maxConnections()));
        case ERROR_STACK_MISMATCH:
          cleanConnectionWithoutNotifyListeners();
          throw new CommStackMismatchException("Disconnected due to comm stack mismatch");
        case ERROR_RECONNECTION_REJECTED:
          fireTransportReconnectionRejectedEvent();
          cleanConnectionWithoutNotifyListeners();
          throw new ReconnectionRejectedException(
                                                  "Reconnection rejected by L2 due to stack not found. Client will be unable to join the cluster again unless rejoin is enabled.");
        case ERROR_REDIRECT_CONNECTION:
          if (this.getProductID().isRedirectEnabled()) {
            throw new TransportRedirect(result.synAck.getErrorContext());
          } else {
            Assert.assertTrue(getProductID().isInternal());
          }
          break;
        case ERROR_PRODUCT_NOT_SUPPORTED:
        default:
          throw new TransportHandshakeException("Disconnected due to transport handshake error: " + result.getError());
      }
    }
  }

  /*
   * Do not trigger reconnection
   */
  private void cleanConnectionWithoutNotifyListeners() {
    List<MessageTransportListener> tl = new ArrayList<MessageTransportListener>(this.getTransportListeners());
    this.removeTransportListeners();
    clearConnection();
    clearConnectionID();
    this.addTransportListeners(tl);
  }

  /**
   * Returns true if the MessageTransport was ever in an open state.
   */
  public synchronized boolean wasOpened() {
    return this.opener != null && this.opener.isDone();
  }

  private synchronized CompletableFuture<NetworkStackID> startOpen() {
    if (this.opener == null) {
      this.opener = new CompletableFuture<>();
      return null;
    } else {
      return this.opener;
    }
  }

  private synchronized void finishOpenWithException(Throwable e) {
    this.opener.completeExceptionally(e);
    this.opener = null;
  }

  private synchronized void finishOpen(NetworkStackID didOpen) {
    this.opener.complete(didOpen);
  }

  private synchronized boolean isOpening() {
    return this.opener != null && !this.opener.isDone();
  }

  @Override
  public void closeEvent(TCConnectionEvent event) {
    if (!isOpening() && !status.isAlive()) {
      return; 
    }
    super.closeEvent(event);
    clearAckWaiter(new IOException("connection closed"));
  }

  @Override
  protected void receiveTransportMessageImpl(WireProtocolMessage message) {
    boolean receive = false;
    if (status.isEstablished()) {
      receive = true;
    } else {
      synchronized (status) {
        if (this.status.isSynSent()) {
          handleSynAck(message);
        } else if (!this.status.isEstablished()) {
          this.getLogger().debug("Ignoring the message received for an Un-Established Connection; " + message.getSource()
                     + "; " + message);
        } else {
          receive = true;
        }
      }
    }
    if (receive) {
      super.receiveToReceiveLayer(message);
    }
  }

  private void handleSynAck(WireProtocolMessage message) {
    if (!verifySynAck(message)) {
      handleHandshakeError(new TransportHandshakeErrorContext(
                                                              "Received a message that was not a SYN_ACK while waiting for SYN_ACK: "
                                                                  + message, TransportHandshakeError.ERROR_HANDSHAKE));
      clearAckWaiter(new IOException("expecting syn ack but got " + message.getClass().getName()));
    } else {
      SynAckMessage synAck = (SynAckMessage) message;
      if (synAck.hasErrorContext()) {
        if (synAck.getErrorType() == TransportHandshakeError.ERROR_STACK_MISMATCH) {
          handleHandshakeError(new TransportHandshakeErrorContext(getCommsStackMismatchErrorMessage(synAck)
                                                                  + "\n\nPLEASE RECONFIGURE THE STACKS",
                                                                  synAck.getErrorType()));
        } else {
          handleHandshakeError(new TransportHandshakeErrorContext(synAck.getErrorContext() + " " + message,
                                                                  synAck.getErrorType()));
        }
      } else if (!getConnectionID().isNewConnection() && getConnectionID().isValid()) {
        // This is a reconnect
        Assert.eval(getConnectionID().equals(synAck.getConnectionId()));
      }
      getConnection().setTransportEstablished();
      setSynAckResult(synAck);
    }
  }

  private synchronized CompletableFuture<SynAckMessage> createAckWaiter() throws TransportHandshakeException {
    if (this.waitForSynAckResult == null) {
      this.waitForSynAckResult = new CompletableFuture<>();
    } else {
      throw new TransportHandshakeException("duplicate handshake");
    }
    return this.waitForSynAckResult;
  }

  private synchronized void clearAckWaiter(Throwable e) {
    if (this.waitForSynAckResult != null) {
      this.waitForSynAckResult.completeExceptionally(e);
      this.waitForSynAckResult = null;
    }
  }

  private synchronized void setSynAckResult(SynAckMessage msg) {
    if (this.waitForSynAckResult != null) {
      this.waitForSynAckResult.complete(msg);
      this.waitForSynAckResult = null;
    }
  }

  /**
   * If communication stacks are mismatched then get the client side communication stack and append in the error message
   */
  private String getCommsStackMismatchErrorMessage(SynAckMessage synAck) {
    String errorMessage = "\n\nLayers Present in Client side communication stack: ";
    // get the names of stack layers present
    errorMessage += getCommunicationStackNames(this);
    errorMessage = "\nTHERE IS A MISMATCH IN THE COMMUNICATION STACKS\n" + synAck.getErrorContext() + errorMessage;
    if ((getCommunicationStackFlags(this) & NetworkLayer.TYPE_OOO_LAYER) != 0) {
      this.getLogger().error(NetworkLayer.ERROR_OOO_IN_CLIENT_NOT_IN_SERVER);
      errorMessage = "\n\n" + NetworkLayer.ERROR_OOO_IN_CLIENT_NOT_IN_SERVER + errorMessage;
    } else {
      this.getLogger().error(NetworkLayer.ERROR_OOO_IN_SERVER_NOT_IN_CLIENT);
      errorMessage = "\n\n" + NetworkLayer.ERROR_OOO_IN_SERVER_NOT_IN_CLIENT + errorMessage;
    }
    return errorMessage;
  }

  private boolean verifySynAck(TCNetworkMessage message) {
    // XXX: yuck.
    return message instanceof TransportHandshakeMessage && ((TransportHandshakeMessage) message).isSynAck();
  }

  /**
   * Builds a protocol stack and tries to make a connection. This is a blocking call.
   * 
   * @throws TCTimeoutException
   */
  HandshakeResult handShake() throws TCTimeoutException, TransportHandshakeException {
    try {
      SynAckMessage synAck = sendSyn().get(TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT, TimeUnit.MILLISECONDS);
      return new HandshakeResult(synAck);
    } catch (TransportHandshakeException t) {
      clearAckWaiter(t);
      throw t;
    } catch (InterruptedException e) {
      clearAckWaiter(e);
      throw new TransportHandshakeException(e);
    } catch (ExecutionException e) {
      clearAckWaiter(e.getCause());
      throw new TransportHandshakeException(e.getCause());
    } catch (TimeoutException e) {
      clearAckWaiter(e);
      throw new TCTimeoutException(e);
    } catch (Throwable t) {
      clearAckWaiter(t);
      throw new TransportHandshakeException(t);
    }
  }

  private Future<SynAckMessage> sendSyn() throws TransportHandshakeException {
    CompletableFuture<SynAckMessage> targetFuture = createAckWaiter();
    synchronized (this.status) {
      if (!this.status.isAlive() || !this.status.isConnected()) {
        logger.warn("transport in the incorrect state {}", this.status);
        clearAckWaiter(new IOException("closed"));
        return targetFuture;
      }
      
      if (this.status.isEstablished() || this.status.isSynSent()) { throw new AssertionError(" ERROR !!! "
                                                                                             + this.status); }
      // get the stack layer list and pass it in
      short stackLayerFlags = getCommunicationStackFlags(this);
      TransportHandshakeMessage syn = this.messageFactory.createSyn(getConnectionID(), getConnection(),
                                                                    stackLayerFlags);
      // send syn message
      try {
        this.sendToConnection(syn);
        if (!targetFuture.isCompletedExceptionally()) {
          this.status.synSent();
        }
      } catch (IOException ioe) {
        logger.warn("trouble syn", ioe);
      }
    }
    
    return targetFuture;
  }

  private void sendAck() throws TransportHandshakeException {
    synchronized (this.status) {
      // DEV-1364 : Connection close might have happened
      if (!this.status.isSynSent()) {
        throw new TransportHandshakeException("Transport is not " + MessageTransportState.STATE_SYN_SENT
                                              + ". Status: " + status);
      }
      TransportHandshakeMessage ack = this.messageFactory.createAck(getConnectionID(), getConnection());
      // send ack message
      try {
        this.sendToConnection(ack);
      } catch (IOException ioe) {
        throw new TransportHandshakeException(ioe);
      }
      status.established();
    }
    fireTransportConnectedEvent();
  }

  protected void openConnection(TCConnection connection) throws TCTimeoutException, TransportHandshakeException,
      MaxConnectionsExceededException, CommStackMismatchException {
    Assert.eval(!isConnected());
    if (wireNewConnection(connection)) {
      try {
        handshakeConnection();
      } catch (TCTimeoutException e) {
        clearConnection();
        clearConnectionID();
        throw e;
      } catch (ReconnectionRejectedException e) {
        throw new TCRuntimeException("Should not happen here: " + e);
      } catch (TransportHandshakeException e) {
        clearConnection();
        clearConnectionID();
        throw e;
      }
    } else {
      throw new TransportHandshakeException("connection closed");
    }
  }

  void reopen(InetSocketAddress serverAddress) throws TCTimeoutException, ReconnectionRejectedException, MaxConnectionsExceededException, CommStackMismatchException, IOException {

    // don't do reconnect if open is still going on
    if (!wasOpened()) {
      this.getLogger().info("Transport never opened. Skip reconnect " + serverAddress);
      return;
    }
    
    reconnect(serverAddress);
  }
  
  void reconnect(InetSocketAddress socket) throws TCTimeoutException, ReconnectionRejectedException, MaxConnectionsExceededException, CommStackMismatchException, IOException {
    TCConnection connection = connect(socket);
      
    Assert.eval(!isConnected());
    if (wireNewConnection(connection)) {
      try {
        handshakeConnection();
        if (!connection.isConnected()) {
          throw new IOException("closed");
        }
      } catch (TCTimeoutException exp) {
        clearConnection();
        throw exp;
      } catch (IOException io) {
        clearConnection();
        throw io;
      }
    }
  }

  private void handshakeConnection() throws TCTimeoutException, MaxConnectionsExceededException,
      TransportHandshakeException, CommStackMismatchException, ReconnectionRejectedException {
    HandshakeResult result = handShake();
    handleHandshakeError(result);
    initConnectionID(result.synAck.getConnectionId());
    sendAck();
    logger.debug("Handshake is complete");
  }

  private String getMaxConnectionsExceededMessage(int maxConnections) {
    return "Your product key only allows maximum " + maxConnections + " clients to connect.";
  }

  TCProtocolAdaptor getProtocolAdapter() {
    return this.wireProtocolAdaptorFactory.newWireProtocolAdaptor(new WireProtocolMessageSink() {
      @Override
      public void putMessage(WireProtocolMessage message) {
        receiveTransportMessage(message);
      }
    });
  }

  private static final class HandshakeResult {
    private final SynAckMessage synAck;

    private HandshakeResult(SynAckMessage synAck) {
      this.synAck = synAck;
    }

    public int maxConnections() {
      return this.synAck.getMaxConnections();
    }

    public boolean hasErrorContext() {
      return this.synAck.isMaxConnectionsExceeded() || this.synAck.hasErrorContext();
    }
    
    public boolean isConnectionValid() {
      return synAck.getConnectionId().isValid();
    }

    public TransportHandshakeError getError() {
      if (this.synAck.isMaxConnectionsExceeded()) {
        return TransportHandshakeError.ERROR_MAX_CONNECTION_EXCEED;
      } else {
        return this.synAck.getErrorType();
      }
    }

  }

  @Override
  protected void fireTransportConnectAttemptEvent() {
    super.fireTransportConnectAttemptEvent();
  }

  @Override
  public boolean isConnected() {
    return super.isConnected();
  }

  // method used for testing
  public void switchLoggerForTesting(ConnectionIdLogger tmpLogger) {
    this.logger = tmpLogger;
  }

  @Override
  public void sendToConnection(TCNetworkMessage message) throws IOException {
    // override just here to satisfy mocking in tests...
    super.sendToConnection(message); 
  }
  
  boolean isRetryOnReconnectionRejected() {
    return this.reconnectionRejectedHandler.isRetryOnReconnectionRejected();
  }
}
