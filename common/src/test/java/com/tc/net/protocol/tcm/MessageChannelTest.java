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
package com.tc.net.protocol.tcm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.net.core.ClearTextSocketEndpointFactory;
import com.tc.net.core.TCConnectionManager;
import com.tc.net.core.TCConnectionManagerImpl;
import com.tc.net.protocol.NetworkStackHarnessFactory;
import com.tc.net.protocol.PlainNetworkStackHarnessFactory;
import com.tc.net.protocol.tcm.msgs.PingMessage;
import com.tc.net.protocol.transport.ConnectionID;
import com.tc.net.protocol.transport.DefaultConnectionIdFactory;
import com.tc.net.protocol.transport.DisabledHealthCheckerConfigImpl;
import com.tc.net.protocol.transport.MessageTransport;
import com.tc.net.protocol.transport.NullConnectionPolicy;
import com.tc.net.protocol.transport.TransportHandshakeErrorHandlerForL1;
import com.tc.net.protocol.transport.TransportHandshakeErrorNullHandler;
import com.tc.net.protocol.transport.TransportHandshakeException;
import com.tc.net.protocol.transport.WireProtocolMessage;
import com.tc.net.protocol.transport.WireProtocolMessageSink;
import com.tc.net.core.ProductID;
import com.tc.test.TCTestCase;
import com.tc.util.Assert;
import com.tc.util.SequenceGenerator;
import com.tc.util.TCTimeoutException;
import com.tc.util.concurrent.ThreadUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/*
 * This test really belongs in the TC Messaging module but it's dependencies
 * currently prevent that.  It needs some heavy refactoring.
 */
/**
 * This is a test case for MessageChannel. XXX: This test could use some work. It's not very coherent and uses sleeps.
 * --Orion 12/19/2005
 */
public class MessageChannelTest extends TCTestCase {
  static final int             ITERATIONS    = 100;
  static final int             WAIT_PERIOD   = 100;
  static final int             WAIT          = ITERATIONS * WAIT_PERIOD;
  static final int             MESSAGE_COUNT = 250;

  Logger logger = LoggerFactory.getLogger(getClass());
  NetworkListener              lsnr;
  InetSocketAddress            serverAddress;
  TCConnectionManager         clientConns;
  TCConnectionManager         serverConns;
  CommunicationsManagerImpl        clientComms;
  CommunicationsManagerImpl        serverComms;
  TCMessageRouter              clientMessageRouter;
  TCMessageRouter              serverMessageRouter;
  ClientMessageChannel         clientChannel;
  MessageSendAndReceiveWatcher clientWatcher;
  MessageSendAndReceiveWatcher serverWatcher;
  AtomicReference<Throwable>   error         = new AtomicReference<Throwable>(null);
  SequenceGenerator            sq            = new SequenceGenerator();

  protected void setUp(ProductID product) throws Exception {
    setUp(product, false);
  }

  protected void setUp(ProductID product, boolean dumbSink) throws Exception {
    setUp(product, new PlainNetworkStackHarnessFactory(),
          new PlainNetworkStackHarnessFactory(), dumbSink);
  }

  protected void setUp(ProductID product, NetworkStackHarnessFactory clientStackHarnessFactory,
                       NetworkStackHarnessFactory serverStackHarnessFactory, boolean dumbServerSink) throws Exception {
    super.setUp();
try {
    clientWatcher = new MessageSendAndReceiveWatcher();
    serverWatcher = new MessageSendAndReceiveWatcher();

    clientMessageRouter = new TCMessageRouterImpl();
    serverMessageRouter = new TCMessageRouterImpl();
    MessageMonitor mm = new NullMessageMonitor();
    clientConns = new TCConnectionManagerImpl("TestCommMgr-client", 0, new ClearTextSocketEndpointFactory());
    clientComms = new CommunicationsManagerImpl(mm, clientMessageRouter,
                                                clientStackHarnessFactory, clientConns, new NullConnectionPolicy(),
                                                new DisabledHealthCheckerConfigImpl(), new TransportHandshakeErrorHandlerForL1(), 
                                                Collections.<TCMessageType, Class<? extends TCAction>>emptyMap(),
                                                Collections.<TCMessageType, GeneratedMessageFactory>emptyMap());

    serverConns = new TCConnectionManagerImpl("TestCommMgr-server", 0, new ClearTextSocketEndpointFactory());
    serverComms = new CommunicationsManagerImpl(mm, serverMessageRouter,
                                                serverStackHarnessFactory, serverConns, new NullConnectionPolicy(),
                                                new DisabledHealthCheckerConfigImpl(), new TransportHandshakeErrorNullHandler(),
                                                Collections.<TCMessageType, Class<? extends TCAction>>emptyMap(),
                                                Collections.<TCMessageType, GeneratedMessageFactory>emptyMap());

    initListener(clientWatcher, serverWatcher, dumbServerSink);
    this.clientChannel = createClientMessageChannel(product);
    this.setUpClientReceiveSink();
} catch (Exception ex) {
  ex.printStackTrace();
  throw ex;
}
  }

  private void initListener(final MessageSendAndReceiveWatcher myClientSenderWatcher,
                            final MessageSendAndReceiveWatcher myServerSenderWatcher, boolean dumbServerSink, int port) throws IOException, TCTimeoutException {
      if (lsnr != null) {
        lsnr.stop();
      }

      serverComms.addClassMapping(TCMessageType.PING_MESSAGE, PingMessage.class);
      ((CommunicationsManagerImpl) serverComms).getMessageRouter().routeMessageType(TCMessageType.PING_MESSAGE,
          new TCMessageSink() {
            @Override
            public void putMessage(TCAction message)
                throws UnsupportedMessageTypeException {
              //System.out.println(message);

              PingMessage ping = (PingMessage) message;
              try {
                message.hydrate();
              } catch (Exception e) {
                setError(e);
              }
              myClientSenderWatcher
                  .addMessageReceived(ping);

              PingMessage pong = ping
                  .createResponse();
              pong.send();
              myServerSenderWatcher
                  .addMessageSent(pong);
            }
          });
      if (dumbServerSink) {
        lsnr = serverComms.createListener(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), (c)->false,
            new DefaultConnectionIdFactory(), false, new WireProtocolMessageSink() {

          @Override
          public void putMessage(WireProtocolMessage message) {
            // Thanks for the message.
            // But i don't give you back anything
            // as i am Dumb.
          }
        }, null, (t)->true);
      } else {
        lsnr = serverComms.createListener(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), (c)->false,
            new DefaultConnectionIdFactory(), (MessageTransport t)->true);
      }
      lsnr.start(new HashSet<>());
      serverAddress = InetSocketAddress.createUnresolved("localhost", lsnr.getBindPort());
  }

  private void initListener(MessageSendAndReceiveWatcher myClientSenderWatcher,
                            MessageSendAndReceiveWatcher myServerSenderWatcher, boolean dumbServerSink) throws IOException, TCTimeoutException {
    initListener(myClientSenderWatcher, myServerSenderWatcher, dumbServerSink, 0);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    final Throwable lastError = error.get();
    if (lastError != null) { throw new Exception(lastError); }

    if (lsnr != null) lsnr.stop();
    if (this.clientChannel != null) this.clientChannel.close();
    if (clientComms != null) clientComms.shutdown();
    if (serverComms != null) serverComms.shutdown();
    if (clientConns != null) clientConns.shutdown();
    if (serverConns != null) serverConns.shutdown();
  }

  public void testAttachments() throws Exception {
    setUp(ProductID.STRIPE);
    try {
      clientChannel.open(serverAddress);
    } catch (Exception e) {
      e.printStackTrace();
    }
    String key = "key";
    MessageChannel channel = createClientMessageChannel(ProductID.STRIPE);
    assertNull(channel.getAttachment(key));
    assertNull(channel.removeAttachment(key));

    Object attachment = new Object();
    Object attachment2 = new Object();
    channel.addAttachment(key, attachment, false);
    assertSame(attachment, channel.getAttachment(key));
    channel.addAttachment(key, attachment, false);
    assertSame(attachment, channel.getAttachment(key));

    channel.addAttachment(key, attachment2, true);
    assertSame(attachment2, channel.getAttachment(key));

    Object removed = channel.removeAttachment(key);
    assertSame(attachment2, removed);

    removed = channel.removeAttachment(key);
    assertNull(removed);
    assertNull(channel.getAttachment(key));
  }

  public void testOpenRaceWithAutoReconnect() throws Exception {
    setUp(ProductID.STRIPE, true);

    Thread t = new Thread() {
      @Override
      public void run() {
        ThreadUtil.reallySleep(WAIT / 10);
        serverComms.getConnectionManager().shutdown();
        System.err.println("closed connections on server side");
      }
    };

    t.start();

    boolean justTimeout = true;
    while (justTimeout) {
      try {
        clientChannel.open(serverAddress);
        fail();
      } catch(ConnectException ce) {
        // missed it
        System.err.println("Unexpected: missed race for first open() : " + ce);
        justTimeout=false;
      } catch (TransportHandshakeException e) {
        // expected;
        System.err.println("Expected: got handshake exception for first open() : " + e);
        justTimeout = false;
      } catch (TCTimeoutException to) {
        System.err.println("Unexpected: timeout for first open() : " + to);
        justTimeout=false;
        //System.out.println(ThreadDumpUtil.getThreadDump());
      }
    }

    try {
      clientChannel.open(serverAddress);
      fail();
    } catch (ConnectException e) {
      // expected
      System.err.println("Expected: Connection Error: " + e);
    }
    clientChannel = null;
  }

  public void testClientSwitchOver() throws Exception {

    clientWatcher = new MessageSendAndReceiveWatcher();
    serverWatcher = new MessageSendAndReceiveWatcher();

    MessageMonitor mm = new NullMessageMonitor();
    
    clientConns = new TCConnectionManagerImpl("TestCommMgr-client-1", 0, new ClearTextSocketEndpointFactory());
    clientComms = new CommunicationsManagerImpl(mm, new PlainNetworkStackHarnessFactory(),
                                                clientConns, 
                                                new NullConnectionPolicy());

    TCConnectionManager serverConns1 = new TCConnectionManagerImpl("TestCommMgr-server-1", 0, new ClearTextSocketEndpointFactory());
    CommunicationsManagerImpl serverComms1 = new CommunicationsManagerImpl(mm,
                                                                       new PlainNetworkStackHarnessFactory(),
                                                                       serverConns1, 
                                                                       new NullConnectionPolicy());
    TCConnectionManager serverConns2 = new TCConnectionManagerImpl("TestCommMgr-server-2", 0, new ClearTextSocketEndpointFactory());
    CommunicationsManagerImpl serverComms2 = new CommunicationsManagerImpl(mm,
                                                                       new PlainNetworkStackHarnessFactory(),
                                                                       serverConns2, new NullConnectionPolicy());

    addCommsMappingAndRouting(clientWatcher, serverWatcher, serverComms1);
    NetworkListener lsnr1 = getListener(clientWatcher, serverWatcher, true, serverComms1);

    addCommsMappingAndRouting(clientWatcher, serverWatcher, serverComms2);
    NetworkListener lsnr2 = getListener(clientWatcher, serverWatcher, false, serverComms2);

    TCConnectionManager clientConns2 = new TCConnectionManagerImpl("TestCommMgr-client-2", 0, new ClearTextSocketEndpointFactory());
    CommunicationsManager clComms = new CommunicationsManagerImpl(mm,
                                                                  new PlainNetworkStackHarnessFactory(),
                                                                  clientConns2, new NullConnectionPolicy());

    this.setUpClientReceiveSink();
    this.clientChannel = createClientMessageChannel(ProductID.STRIPE, clComms);

    try {
      clientChannel.open(Arrays.asList(InetSocketAddress.createUnresolved("localhost", lsnr1.getBindPort()),
                                       InetSocketAddress.createUnresolved("localhost", lsnr2.getBindPort())));
    } catch (TCTimeoutException e) {
      Assert.eval("This is not suppose to happen", false);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
    Assert.eval(clientChannel.isConnected());
    lsnr1.stop();
    lsnr2.stop();
    serverComms1.shutdown();
    serverComms2.shutdown();
    clComms.shutdown();
    clientConns2.shutdown();
  }

  private NetworkListener getListener(MessageSendAndReceiveWatcher clientWatcher2,
                                      MessageSendAndReceiveWatcher serverWatcher2, boolean dumbServerSink,
                                      CommunicationsManagerImpl serverComms1) throws IOException {

    NetworkListener rv;
    if (dumbServerSink) {
      rv = serverComms1.createListener(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), (c)->false,
                                       new DefaultConnectionIdFactory(), false, new WireProtocolMessageSink() {

                                         @Override
                                        public void putMessage(WireProtocolMessage message) {
                                           // Thanks for the message.
                                           // But i don't give you back anything
                                           // as i am Dumb.
                                         }
                                       }, null, (t)->true);
    } else {
      rv = serverComms1.createListener(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), (c)->false,
                                       new DefaultConnectionIdFactory(), (MessageTransport t)->true);
    }

    rv.start(new HashSet<ConnectionID>());
    return rv;
  }

  private void addCommsMappingAndRouting(final MessageSendAndReceiveWatcher clientWatcher2,
                                         final MessageSendAndReceiveWatcher serverWatcher2,
                                         CommunicationsManager serverComms1) {
    serverComms1.addClassMapping(TCMessageType.PING_MESSAGE, PingMessage.class);
    ((CommunicationsManagerImpl) serverComms1).getMessageRouter().routeMessageType(TCMessageType.PING_MESSAGE,
        new TCMessageSink() {
          @Override
          public void putMessage(TCAction message)
              throws UnsupportedMessageTypeException {
            //System.out.println(message);

            PingMessage ping = (PingMessage)message;
            try {
              message.hydrate();
            } catch (Exception e) {
              setError(e);
            }
            clientWatcher2
                .addMessageReceived(ping);

            PingMessage pong = ping
                .createResponse();
            pong.send();
            serverWatcher2
                .addMessageSent(pong);
          }
        });
  }

  private void waitForMessages(int count) throws InterruptedException {
    waitForArrivalOrFail(clientWatcher, count);
    waitForArrivalOrFail(serverWatcher, count);

    String msg = "expected: " + count + ", client sent: " + clientWatcher.sent() + ", client received: "
                 + clientWatcher.received() + ", server sent: " + serverWatcher.sent() + ", server received: "
                 + serverWatcher.received();

    assertEquals(msg, count, clientWatcher.sent());
    assertEquals(msg, count, clientWatcher.received());
    assertEquals(msg, count, serverWatcher.sent());
    assertEquals(msg, count, serverWatcher.received());
  }

  public void testManualReconnectAfterFailure() throws Exception {
    setUp(ProductID.SERVER);

    int port = lsnr.getBindPort();

    lsnr.stop();
    serverComms.getConnectionManager().closeAllConnections();
    clientComms.getConnectionManager().closeAllConnections();

    for (int i = 0; i < 10; i++) {
      try {
        clientChannel.open(serverAddress);
        fail("Should have thrown an exception");
      } catch (TCTimeoutException e) {
        // expected
      } catch (UnknownHostException e) {
        fail(e.getMessage());
      } catch (IOException e) {
        // expected
      }

      assertFalse(clientChannel.isConnected());
    }

    initListener(this.clientWatcher, this.serverWatcher, false, port);
    clientChannel.open(InetSocketAddress.createUnresolved("localhost", port));
    assertTrue(clientChannel.isConnected());
  }

  public void testSendAfterDisconnect() throws Exception {
    setUp(ProductID.SERVER);
    clientChannel.open(serverAddress);

    createAndSendMessage();
    waitForArrivalOrFail(clientWatcher, 1);
    waitForArrivalOrFail(serverWatcher, 1);

    sendMessagesWhileDisconnected(MESSAGE_COUNT, 25);

    // don't explicitly need to do this, but if we wait, it's possible an error will happen on another thread
    ThreadUtil.reallySleep(5000);
  }

  public void testZeroMaxRetriesDoesntAutoreconnect() throws Exception {
    setUp(ProductID.SERVER);
    assertEquals(0, clientChannel.getConnectAttemptCount());
    assertEquals(0, clientChannel.getConnectCount());

    clientChannel.open(serverAddress);
    assertEquals(1, clientChannel.getConnectAttemptCount());
    assertEquals(1, clientChannel.getConnectCount());
    clientComms.getConnectionManager().closeAllConnections();
    ThreadUtil.reallySleep(5000);
    assertEquals(1, clientChannel.getConnectAttemptCount());
    assertEquals(1, clientChannel.getConnectCount());
  }

  public void testNegativeMaxRetriesAlwaysReconnects() throws Exception {
    setUp(ProductID.STRIPE);

    assertEquals(0, clientChannel.getConnectCount());
    assertEquals(0, clientChannel.getConnectAttemptCount());

    clientChannel.open(serverAddress);

    assertEquals(1, clientChannel.getConnectCount());
    assertEquals(1, clientChannel.getConnectAttemptCount());

    lsnr.stop();
    assertEquals(0, serverComms.getAllListeners().length);

    clientComms.getConnectionManager().closeAllConnections();
    int count = clientChannel.getConnectAttemptCount();
    ThreadUtil.reallySleep(WAIT * 4);
    assertTrue(clientChannel.getConnectAttemptCount() + " vs " + count, clientChannel.getConnectAttemptCount() > count);
    assertEquals(1, clientChannel.getConnectCount());
  }

  // public void testSendBeforeOpen() throws Exception {
  // setUp(0);
  // PingMessage ping = createMessage();
  // assertTrue(clientChannel.getStatus().isClosed());
  // try {
  // ping.send();
  // fail("Should have thrown an assertion error");
  // } catch (TCAssertionError e) {
  // // expected
  // }
  // }
  //
  // public void testSendAfterClose() throws Exception {
  // setUp(0);
  // clientChannel.open();
  // assertTrue(clientChannel.getStatus().isOpen());
  //
  // PingMessage ping = createMessage();
  // clientChannel.close();
  // assertTrue(clientChannel.isClosed());
  //
  // try {
  // // send should fail
  // ping.send();
  // fail("should have thrown an exception");
  // } catch (TCAssertionError err) {
  // // expected
  // }
  // }

  public void testGetStatus() throws Exception {
    setUp(ProductID.SERVER);
    clientChannel.open(serverAddress);
    assertTrue(clientChannel.isOpen());
    clientChannel.close();
    assertTrue(clientChannel.isClosed());
  }

  public void testSend() throws Exception {
    setUp(ProductID.SERVER);
    clientChannel.open(serverAddress);
    int count = 100;
    List<PingMessage> messages = new LinkedList<PingMessage>();
    for (int i = 0; i < count; i++) {
      messages.add(createAndSendMessage());
    }
    waitForMessages(count);

  }

  public void testSocketInfo() throws Exception {
    setUp(ProductID.SERVER);

    assertNull(clientChannel.getRemoteAddress());
    assertNull(clientChannel.getLocalAddress());

    clientChannel.open(serverAddress);
    createAndSendMessage();
    waitForMessages(1);

    InetSocketAddress clientRemote = clientChannel.getRemoteAddress();
    InetSocketAddress clientLocal = clientChannel.getLocalAddress();

    MessageChannelInternal[] serverChannels = lsnr.getChannelManager().getChannels();
    assertEquals(1, serverChannels.length);
    MessageChannelInternal serverChannel = serverChannels[0];

    InetSocketAddress serverRemote = serverChannel.getRemoteAddress();
    InetSocketAddress serverLocal = serverChannel.getLocalAddress();

    assertEquals(clientRemote, serverLocal);
    assertEquals(clientLocal, serverRemote);
  }

  private PingMessage createAndSendMessage() {
    PingMessage ping = createMessage();
    clientWatcher.addMessageSent(ping);
    ping.send();
    return ping;
  }

  private static void waitForArrivalOrFail(MessageSendAndReceiveWatcher watcher, int count) throws InterruptedException {
    int i = 0;
    while (!watcher.allReceived() || (watcher.sent() < count) || (watcher.received() < count)) {
      if (i == ITERATIONS) {
        fail((watcher.sent() - watcher.received()) + " messages of " + watcher.sent()
             + " messages total failed to arrive in " + ITERATIONS + " iterations of " + WAIT_PERIOD + " ms. waiting.");
      }

      Thread.sleep(WAIT_PERIOD);
      i++;
    }
  }
  
  private ClientMessageChannel createClientMessageChannel(ProductID product) {
    return createClientMessageChannel(product, clientComms);
  }

  private ClientMessageChannel createClientMessageChannel(ProductID product, CommunicationsManager clComms) {
    clComms.addClassMapping(TCMessageType.PING_MESSAGE, PingMessage.class);
    ClientMessageChannel ch = clientComms.createClientChannel(product, WAIT);
    return ch;
  }

  private PingMessage createMessage() {
    PingMessage ping = (PingMessage) clientChannel.createMessage(TCMessageType.PING_MESSAGE);
    ping.initialize(sq.getNextSequence());
    return ping;
  }

  private void sendMessagesWhileDisconnected(int count, int afterCount) throws InterruptedException {
    Random rnd = new Random();
    final int closeCount = rnd.nextInt(count);
    final boolean serverClose = rnd.nextBoolean();

    Thread thread = null;

    for (int i = 0; i < count; i++) {
      if (i == closeCount) {
        // close down the connection in a seperate thread to increase the timing randomness
        thread = new Thread("Connection closer thread") {
          @Override
          public void run() {
            try {
              if (serverClose) {
                logger.info("Initiating close on the SERVER side...");
                serverComms.getConnectionManager().closeAllConnections();
              } else {
                logger.info("Initiating close on the CLIENT side...");
                clientComms.getConnectionManager().closeAllConnections();
              }
            } catch (Throwable t) {
              setError(t);
            }
          }
        };
        Thread.sleep(rnd.nextInt(25) + 10);
        thread.setDaemon(true);
        thread.start();
      }

      createAndSendMessage();
    }

    thread.join(WAIT);
    assertFalse(thread.isAlive());

    // make sure we send messages after the connection has actually closed for good measure
    for (int i = 0; i < afterCount; i++) {
      createAndSendMessage();
    }
  }

  private boolean waitUntilReconnected() {
    final long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < WAIT) {
      if (clientChannel.isConnected()) return true;
      try {
        Thread.sleep(WAIT_PERIOD);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  private void setUpClientReceiveSink() {
    final MessageSendAndReceiveWatcher myServerSenderWatcher = this.serverWatcher;
    ((CommunicationsManagerImpl) clientComms).getMessageRouter().routeMessageType(TCMessageType.PING_MESSAGE,
        new TCMessageSink() {
          @Override
          public void putMessage(TCAction message)
              throws UnsupportedMessageTypeException {
            try {
              PingMessage ping = (PingMessage)message;
              ping.hydrate();
              // System.out.println("CLIENT RECEIVE: "
              // + ping.getSequence());
            } catch (Exception e) {
              setError(e);
            }
            PingMessage ping = (PingMessage)message;
            myServerSenderWatcher
                .addMessageReceived(ping);
          }
        });
  }

  private void setError(Throwable t) {
    synchronized (System.err) {
      System.err.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S").format(new Date())
                         + ": Exception Thrown in thread [" + Thread.currentThread().getName() + "]");
      t.printStackTrace(System.err);
    }
    error.set(t);
  }

  public static class MessageSendAndReceiveWatcher {

    private final Set<Long> sentSequences     = new HashSet<Long>();
    private final Set<Long> receivedSequences = new HashSet<Long>();

    public synchronized void addMessageSent(PingMessage sent) {
      sentSequences.add(sent.getSequence());
    }

    public synchronized void addMessageReceived(PingMessage received) {
      receivedSequences.add(received.getSequence());
    }

    public int sent() {
      return sentSequences.size();
    }

    public int received() {
      return receivedSequences.size();
    }

    public synchronized boolean allReceived() {
      return receivedSequences.containsAll(sentSequences);
    }
  }
}
