package io.servicefabric.transport;

import static com.google.common.base.Throwables.propagate;
import static io.servicefabric.transport.TransportData.META_ORIGIN_ENDPOINT;
import static io.servicefabric.transport.TransportData.META_ORIGIN_ENDPOINT_ID;
import static io.servicefabric.transport.TransportEndpoint.from;
import static org.junit.Assert.*;

import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.*;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;
import rx.functions.Action1;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.ProtostuffFrameHandlerFactory;
import io.servicefabric.transport.protocol.ProtostuffMessageDeserializer;
import io.servicefabric.transport.protocol.ProtostuffMessageSerializer;

@SuppressWarnings("unchecked")
public class SocketChannelTransportFactoryTest {
	static final Logger LOGGER = LoggerFactory.getLogger(SocketChannelTransportFactoryTest.class);

	Transport client;
	Transport server;

	@After
	public void tearDown() throws Exception {
		if (client != null) {
			SettableFuture<Void> close = SettableFuture.create();
			client.stop(close);
			close.get(1, TimeUnit.SECONDS);
			pause(100);
		}
		if (server != null) {
			SettableFuture<Void> close = SettableFuture.create();
			server.stop(close);
			close.get(1, TimeUnit.SECONDS);
			pause(100);
		}
	}

	@Test
	public void testInvalidHandshake() throws Exception {
		for (int i = 0; i < 10; i++) {
			System.err.println("### iter=" + i);
			try {
				TransportEndpoint clientEndpoint = clientEndpoint();
				TransportEndpoint serverEndpoint = serverEndpoint();

				Map crappyMetadata = new HashMap<>();
				crappyMetadata.put("crap" + i, "crap" + i);

				client = TF(clientEndpoint, crappyMetadata);
				server = TF(serverEndpoint, crappyMetadata);

				// attempt to send message from client to server
				SettableFuture<Void> send0 = SettableFuture.create();
				ITransportChannel transport0 = client.to(serverEndpoint);
				transport0.send(new Message("q"), send0);
				try {
					send0.get(1, TimeUnit.SECONDS);
					fail();
				} catch (ExecutionException e) {
					TransportException cause = (TransportException) e.getCause();
				}

				// attempt to send message from server to client
				SettableFuture<Void> send1 = SettableFuture.create();
				ITransportChannel transport1 = server.to(clientEndpoint);
				transport1.send(new Message("q"), send1);
				try {
					send1.get(1, TimeUnit.SECONDS);
					fail();
				} catch (ExecutionException e) {
					TransportException cause = (TransportException) e.getCause();
				}
			} finally {
				if (client != null) {
					SettableFuture<Void> close = SettableFuture.create();
					client.stop(close);
					close.get(1, TimeUnit.SECONDS);
					pause(100);
				}
				if (server != null) {
					SettableFuture<Void> close = SettableFuture.create();
					server.stop(close);
					close.get(1, TimeUnit.SECONDS);
					pause(100);
				}
			}
		}
	}

	@Test
	public void testCloseSequentally() throws Exception {
		client = TF(clientEndpoint());

		ITransportChannel transport0 = client.to(serverEndpoint());
		SettableFuture<Void> f0 = SettableFuture.create();
		transport0.close(f0);
		f0.get();

		// assert different transport objects
		assertNotSame(transport0, client.to(serverEndpoint()));
	}

	@Test
	public void testCloseSequentallyFast() throws Exception {
		client = TF(clientEndpoint());

		ITransportChannel transport0 = client.to(serverEndpoint());
		transport0.close(null);

		// assert different transport objects
		assertNotSame(transport0, client.to(serverEndpoint()));
	}

	@Test
	public void testInteractWithClosedConnector() throws Exception {
		TransportEndpoint clientEndpoint = clientEndpoint();
		TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint);
		server = TF(serverEndpoint);

		// create transport
		ITransportChannel transport = client.to(serverEndpoint);
		// assert connection and message send successfully
		SettableFuture<Void> send0 = SettableFuture.create();
		transport.send(new Message("q"), send0);
		send0.get(1, TimeUnit.SECONDS);

		// close and send message again
		SettableFuture<Void> close = SettableFuture.create();
		transport.close(close);
		close.get();
		// wait a bit
		pause(100);

		// assert cause is TransportClosedException
		SettableFuture<Void> send1 = SettableFuture.create();
		transport.send(new Message("q"), send1);
		try {
			send1.get(1, TimeUnit.SECONDS);
			fail();
		} catch (ExecutionException e) {
			TransportClosedException cause = (TransportClosedException) e.getCause();
		}
	}

	@Test
	public void testInteractWithClosedConnectorFast() throws Exception {
		TransportEndpoint clientEndpoint = clientEndpoint();
		TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint);
		server = TF(serverEndpoint);

		// create transport
		ITransportChannel transport = client.to(serverEndpoint);
		// assert connection and message send successfully
		SettableFuture<Void> send0 = SettableFuture.create();
		transport.send(new Message("q"), send0);
		send0.get(1, TimeUnit.SECONDS);

		// close and send message again
		transport.close(null);

		// assert cause is TransportClosedException
		SettableFuture<Void> send1 = SettableFuture.create();
		transport.send(new Message("q"), send1);
		try {
			send1.get(1, TimeUnit.SECONDS);
			fail();
		} catch (ExecutionException e) {
			TransportClosedException cause = (TransportClosedException) e.getCause();
		}
	}

	@Test
	public void testInteractWithNoConnection() throws Exception {
		client = TF(clientEndpoint());

		// create transport and wait a bit so it determine that there's no connection
		ITransportChannel transport = client.to(serverEndpoint());
		pause(3000);

		// assert cause is ConnectException
		SettableFuture<Void> send0 = SettableFuture.create();
		transport.send(new Message("q"), send0);
		try {
			send0.get(1, TimeUnit.SECONDS);
			fail();
		} catch (ExecutionException e) {
			ConnectException cause = (ConnectException) e.getCause();
		}

		// assert cause is ConnectException
		SettableFuture<Void> send1 = SettableFuture.create();
		transport.send(new Message("q"), send1);
		try {
			send1.get(1, TimeUnit.SECONDS);
			fail();
		} catch (ExecutionException e) {
			ConnectException cause = (ConnectException) e.getCause();
		}
	}

	@Test
	public void testInteractWithNoConnectionFast() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.err.println("### iter=" + i);
            client = TF(clientEndpoint());

            // create transport and don't wait just send message
            ITransportChannel transport = client.to(serverEndpoint());
            SettableFuture<Void> send0 = SettableFuture.create();
            transport.send(new Message("q"), send0);
            try {
                send0.get(3, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                ConnectException cause = (ConnectException) e.getCause();
            }
            // send second message: no connection yet and it's clear that there's no connection
            SettableFuture<Void> send1 = SettableFuture.create();
            transport.send(new Message("q"), send1);
            try {
                send1.get(3, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                ConnectException cause = (ConnectException) e.getCause();
            }

            if (client != null) {
                SettableFuture<Void> close = SettableFuture.create();
                client.stop(close);
                close.get(1, TimeUnit.SECONDS);
                pause(100);
            }
        }
    }

	@Test
	public void testPingPongClientTFListenAndServerTFListen() throws Exception {
		final TransportEndpoint clientEndpoint = clientEndpoint();
		final TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint);
		server = TF(serverEndpoint);

		server.listen().subscribe(new Action1<TransportMessage>() {
			@Override
			public void call(TransportMessage transportMessage) {
				TransportEndpoint endpoint = transportMessage.originEndpoint();
				assertEquals("Expected clientEndpoint", clientEndpoint, endpoint);
				send(server, endpoint, new Message(null, TransportHeaders.QUALIFIER, "hi client"));
			}
		});

		//final ValueLatch<Message> latch = new ValueLatch<>();
		final SettableFuture<TransportMessage> transportMessageFuture = SettableFuture.create();
		client.listen().subscribe(new Action1<TransportMessage>() {
			@Override
			public void call(TransportMessage transportMessage) {
				transportMessageFuture.set(transportMessage);
			}
		});

		send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "hello server"));

		TransportMessage transportMessage = transportMessageFuture.get(3, TimeUnit.SECONDS);
		Message result = transportMessage.message();
		assertNotNull("No response from serverEndpoint", result);
		assertEquals("hi client", result.header(TransportHeaders.QUALIFIER));
	}

	@Test
	public void testConnectorSendOrder1Thread() throws Exception {
		TransportEndpoint clientEndpoint = clientEndpoint();
		TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint, 100);
		server = TF(serverEndpoint, 100);

		int total = 1000;
		ITransportChannel transport0 = null;
		for (int i = 0; i < 10; i++) {
			System.err.println("### iter=" + i);
			ITransportChannel transport1 = client.to(serverEndpoint);
			assertNotSame(transport1, transport0);
			transport0 = transport1;

			final List<Message> received = new ArrayList<>();
			final CountDownLatch latch = new CountDownLatch(total);
			server.listen().subscribe(new Action1<TransportMessage>() {
				@Override
				public void call(TransportMessage transportMessage) {
					received.add(transportMessage.message());
					latch.countDown();
				}
			});

			for (int j = 0; j < total; j++) {
				SettableFuture<Void> send = SettableFuture.create();
				transport1.send(new Message(null, TransportHeaders.QUALIFIER, "q" + j), send);
				try {
					send.get(3, TimeUnit.SECONDS);
				} catch (Exception e) {
					System.err.println("### j=" + j);
					propagate(e);
				}
			}

			latch.await(20, TimeUnit.SECONDS);
			{
				SettableFuture<Void> close = SettableFuture.create();
				transport1.close(close);
				close.get(1, TimeUnit.SECONDS);
			}
			pause(100); // wait a bit so close could recognized on other side

			assertSendOrder(total, received);
		}
	}

	@Test
	public void testConnectorSendOrder4Thread() throws Exception {
		TransportEndpoint clientEndpoint = clientEndpoint();
		final TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint, 100);
		server = TF(serverEndpoint, 100);

		final int total = 1000;
		for (int i = 0; i < 10; i++) {
			System.err.println("### iter=" + i);
			ExecutorService exec = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setDaemon(true).build());

			final List<Message> received = new ArrayList<>();
			final CountDownLatch latch = new CountDownLatch(4 * total);
			server.listen().subscribe(new Action1<TransportMessage>() {
				@Override
				public void call(TransportMessage transportMessage) {
					received.add(transportMessage.message());
					latch.countDown();
				}
			});

			Future<Void> f0 = exec.submit(sender(0, serverEndpoint, total));
			Future<Void> f1 = exec.submit(sender(1, serverEndpoint, total));
			Future<Void> f2 = exec.submit(sender(2, serverEndpoint, total));
			Future<Void> f3 = exec.submit(sender(3, serverEndpoint, total));

			f0.get(3, TimeUnit.SECONDS);
			f1.get(3, TimeUnit.SECONDS);
			f2.get(3, TimeUnit.SECONDS);
			f3.get(3, TimeUnit.SECONDS);

			latch.await(20, TimeUnit.SECONDS);
			{
				SettableFuture<Void> close = SettableFuture.create();
				client.to(serverEndpoint).close(close);
				close.get(1, TimeUnit.SECONDS);
			}
			pause(100); // wait a bit so close could recognized on other side
			exec.shutdownNow();

			assertSenderOrder(0, total, received);
			assertSenderOrder(1, total, received);
			assertSenderOrder(2, total, received);
			assertSenderOrder(3, total, received);
		}
	}

	@Test
	public void testNetworkSettings() throws InterruptedException {
		TransportEndpoint clientEndpoint = clientEndpoint();
		TransportEndpoint serverEndpoint = serverEndpoint();

		client = TF(clientEndpoint);
		server = TF(serverEndpoint);

		int lostPercent = 50;
		int mean = 0;
		client.<SocketChannelPipelineFactory> getPipelineFactory().setNetworkSettings(serverEndpoint, lostPercent, mean);

		final List<Message> serverMessageList = new ArrayList<>();
		server.listen().subscribe(new Action1<TransportMessage>() {
			@Override
			public void call(TransportMessage transportMessage) {
				serverMessageList.add(transportMessage.message());
			}
		});

		ITransportChannel transport = client.to(serverEndpoint);
		int total = 1000;
		for (int i = 0; i < total; i++) {
			transport.send(new Message("q" + i), null);
		}

		pause(1000);

		int expectedMax = total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
		int size = serverMessageList.size();
		assertTrue("expectedMax=" + expectedMax + ", actual size=" + size, size < expectedMax);
	}

    @Test
    public void testPingPongOnSingleChannel() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        server = TF(serverEndpoint);
        client = TF(clientEndpoint);

        server.listen().buffer(2).subscribe(new Action1<List<TransportMessage>>() {
            @Override
            public void call(List<TransportMessage> messages) {
                for (TransportMessage message : messages) {
                    Message echo = new Message("echo/" + message.message().header(TransportHeaders.QUALIFIER));
                    message.originChannel().send(echo, null);
                }
            }
        });

        final SettableFuture<List<TransportMessage>> targetFuture = SettableFuture.create();
        client.listen().buffer(2).subscribe(new Action1<List<TransportMessage>>() {
            @Override
            public void call(List<TransportMessage> messages) {
                targetFuture.set(messages);
            }
        });

        ITransportChannel transport = client.to(serverEndpoint);
        transport.send(new Message("q1"), null);
        transport.send(new Message("q2"), null);

        List<TransportMessage> target = targetFuture.get(1, TimeUnit.SECONDS);
        assertNotNull(target);
        assertEquals(2, target.size());
    }

    @Test
    public void testPingPongOnSeparateChannel() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        server = TF(serverEndpoint);
        client = TF(clientEndpoint);


        server.listen().buffer(2).subscribe(new Action1<List<TransportMessage>>() {
            @Override
            public void call(List<TransportMessage> messages) {
                for (TransportMessage message : messages) {
                    Message echo = new Message("echo/" + message.message().header(TransportHeaders.QUALIFIER));
                    server.to(message.originEndpoint()).send(echo, null);
                }
            }
        });

        final SettableFuture<List<TransportMessage>> targetFuture = SettableFuture.create();
        client.listen().buffer(2).subscribe(new Action1<List<TransportMessage>>() {
            @Override
            public void call(List<TransportMessage> messages) {
                targetFuture.set(messages);
            }
        });

        ITransportChannel transport = client.to(serverEndpoint);
        transport.send(new Message("q1"), null);
        transport.send(new Message("q2"), null);

        List<TransportMessage> target = targetFuture.get(1, TimeUnit.SECONDS);
        assertNotNull(target);
        assertEquals(2, target.size());
    }

    @Test
    public void testCompleteObserver() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        server = TF(serverEndpoint);
        client = TF(clientEndpoint);

        final ITransportChannel transport = client.to(serverEndpoint);
        final SettableFuture<Boolean> completeLatch = SettableFuture.create();
        final SettableFuture<Message> messageLatch = SettableFuture.create();

        server.listen().subscribe(new Subscriber<TransportMessage>() {
            @Override
            public void onCompleted() {
                completeLatch.set(true);
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onNext(TransportMessage transportMessage) {
                messageLatch.set(transportMessage.message());
            }
        });

        SettableFuture<Void> send = SettableFuture.create();
        transport.send(new Message("q"), send);
        send.get(1, TimeUnit.SECONDS);

        assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

        SettableFuture<Void> close = SettableFuture.create();
        server.stop(close);
        close.get();

        assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testObserverThrowsException() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        server = TF(serverEndpoint);
        client = TF(clientEndpoint);

        final ITransportChannel transport = client.to(serverEndpoint);

        server.listen().subscribe(new Action1<TransportMessage>() {
            @Override
            public void call(TransportMessage transportMessage) {
                String qualifier = transportMessage.message().header(TransportHeaders.QUALIFIER);
                if (qualifier.startsWith("throw")) {
                    throw new RuntimeException("" + transportMessage);
                }
                if (qualifier.startsWith("q")) {
                    Message echo = new Message("echo/" + transportMessage.message().header(TransportHeaders.QUALIFIER));
                    transportMessage.originChannel().send(echo, null);
                }
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                throwable.printStackTrace();
            }
        });

        // send "throw" and raise exception on server subscriber
        final SettableFuture<TransportMessage> transportMessageFuture0 = SettableFuture.create();
        client.listen().subscribe(new Action1<TransportMessage>() {
            @Override
            public void call(TransportMessage transportMessage) {
                transportMessageFuture0.set(transportMessage);
            }
        });
        transport.send(new Message("throw"), null);
        TransportMessage transportMessage0 = null;
        try {
            transportMessage0 = transportMessageFuture0.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // ignore since expected behavior
        }
        assertNull(transportMessage0);

        // send normal message and check whether server subscriber is broken (no response)
        final SettableFuture<TransportMessage> transportMessageFuture1 = SettableFuture.create();
        client.listen().subscribe(new Action1<TransportMessage>() {
            @Override
            public void call(TransportMessage transportMessage) {
                transportMessageFuture1.set(transportMessage);
            }
        });
        transport.send(new Message("q"), null);
        TransportMessage transportMessage1 = null;
        try {
            transportMessage1 = transportMessageFuture1.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // ignore since expected behavior
        }
        assertNull(transportMessage1);
    }

    @Test
    public void testBlockAndUnblockTraffic() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        client = TF(clientEndpoint);
        server = TF(serverEndpoint);

        server.listen().subscribe(new Action1<TransportMessage>() {
            @Override
            public void call(TransportMessage transportMessage) {
                transportMessage.originChannel().send(transportMessage.message(), null);
            }
        });

        final List<Message> resp = new ArrayList<>();
        client.listen().subscribe(new Action1<TransportMessage>() {
            @Override
            public void call(TransportMessage transportMessage) {
                resp.add(transportMessage.message());
            }
        });

        // test at unblocked transport
        send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "q/unblocked"));

        // then block client->server messages
        pause(1000);
        client.<SocketChannelPipelineFactory> getPipelineFactory().blockMessagesTo(serverEndpoint);
        send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "q/blocked"));

        pause(1000);
        assertEquals(1, resp.size());
        assertEquals("q/unblocked", resp.get(0).header(TransportHeaders.QUALIFIER));
    }

    @Test
    public void testSendMailboxBecomingFull() throws Exception {
        TransportEndpoint clientEndpoint = clientEndpoint();
        TransportEndpoint serverEndpoint = serverEndpoint();

        client = TF(clientEndpoint, 1);
        server = TF(serverEndpoint, 1);

        client.to(serverEndpoint).send(new Message(null, TransportHeaders.QUALIFIER, "ping0"), null);

        SettableFuture<Void> send1 = SettableFuture.create();
        client.to(serverEndpoint).send(new Message(null, TransportHeaders.QUALIFIER, "ping1"), send1);
        try {
            send1.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            TransportMessageException cause = (TransportMessageException) e.getCause();
        }
    }

	private TransportEndpoint serverEndpoint() {
		return from("tcp://localhost:49255");
	}

	private TransportEndpoint clientEndpoint() {
		return from("tcp://localhost:49355");
	}

	private void pause(int millis) throws InterruptedException {
		Thread.sleep(millis);
	}

	private void assertSendOrder(int total, List<Message> received) {
		ArrayList<Message> messages = new ArrayList<>(received);
		assertEquals(total, messages.size());
		for (int k = 0; k < total; k++) {
			assertEquals("q" + k, messages.get(k).header(TransportHeaders.QUALIFIER));
		}
	}

	private Callable<Void> sender(final int id, final TransportEndpoint endpoint, final int total) {
		return new Callable<Void>() {
			public Void call() throws Exception {
				for (int j = 0; j < total; j++) {
					String correlationId = id + "/" + j;
					SettableFuture<Void> send = SettableFuture.create();
					client.to(endpoint).send(new Message(null, TransportHeaders.QUALIFIER, "q", TransportHeaders.CORRELATION_ID, correlationId), send);
					try {
						send.get(3, TimeUnit.SECONDS);
					} catch (Exception e) {
						System.err.println("### j=" + j);
						propagate(e);
					}
				}
				return null;
			}
		};
	}

	private void assertSenderOrder(int id, int total, List<Message> received) {
		ArrayList<Message> messages = new ArrayList<>(received);
		ArrayListMultimap<Integer, Message> group = ArrayListMultimap.create();
		for (Message message : messages) {
			group.put(Integer.valueOf(message.header(TransportHeaders.CORRELATION_ID).split("/")[0]), message);
		}
		assertEquals(total, group.get(id).size());
		for (int k = 0; k < total; k++) {
			assertEquals(id + "/" + k, group.get(id).get(k).header(TransportHeaders.CORRELATION_ID));
		}
	}

	private void send(ITransport from, final TransportEndpoint to, final Message msg) {
		final ITransportChannel transport = from.to(to);
		final SettableFuture<Void> f = SettableFuture.create();
		f.addListener(new Runnable() {
			@Override
			public void run() {
				if (f.isDone()) {
					try {
						f.get();
					} catch (Exception e) {
						LOGGER.error("Failed to send {} to {}, transport: {}, cause: {}", msg, to, transport, e.getCause());
					}
				}
			}
		}, MoreExecutors.directExecutor());
		transport.send(msg, null);
	}

	private Transport TF(TransportEndpoint endpoint, Map localMetadata, int sendHwm) {
		Transport tf = new Transport(endpoint);
		tf.setConnectTimeout(1000);
		tf.setLocalMetadata(localMetadata);
		tf.setSendHwm(sendHwm);
		tf.setPipelineFactory(SocketChannelPipelineFactory.builder()
				.set(new ProtostuffMessageDeserializer())
				.set(new ProtostuffMessageSerializer())
				.set(new ProtostuffFrameHandlerFactory())
				.useNetworkEmulator()
				.build());
		tf.start();
		return tf;
	}

	private Transport TF(TransportEndpoint endpoint, Map localMetadata) {
		return TF(endpoint, localMetadata, 1000);
	}

	private Transport TF(TransportEndpoint endpoint, int sendHwm) {
		Map localMetadata = new HashMap<>();
		localMetadata.put(META_ORIGIN_ENDPOINT, endpoint);
		localMetadata.put(META_ORIGIN_ENDPOINT_ID, UUID.randomUUID().toString());
		return TF(endpoint, localMetadata, sendHwm);
	}

	private Transport TF(TransportEndpoint endpoint) {
		Map localMetadata = new HashMap<>();
		localMetadata.put(META_ORIGIN_ENDPOINT, endpoint);
		localMetadata.put(META_ORIGIN_ENDPOINT_ID, UUID.randomUUID().toString());
		return TF(endpoint, localMetadata, 1000);
	}
}
