package io.scalecube.transport.aeron;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class EchoServer implements Closeable {

    private static final int ECHO_STREAM_ID = 2222;
    private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);
    private final MediaDriver driver;
    private final Aeron aeron;
    private final InetSocketAddress local_address;
    private static ConcurrentMap<Integer, ServerClient> clients = new ConcurrentHashMap<>();


    public EchoServer(MediaDriver driver, Aeron aeron, InetSocketAddress local_address) {
        this.driver = driver;
        this.aeron = aeron;
        this.local_address = local_address;
    }

    public static void main(
            final String[] args)
            throws Exception
    {
        if (args.length < 2) {
            LOG.error("usage: directory local-address local-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final Integer local_port = Integer.valueOf(args[1]);

        final InetSocketAddress local_address =
                new InetSocketAddress("127.0.0.1", local_port);

        try (final EchoServer server = create(directory, local_address)) {
            server.run();
        }
    }

    public void run() throws Exception{
        try(Subscription sub = this.setupSubscription()){
            runLoop(sub);
        }
    }

    public static EchoServer create(final Path media_directory, InetSocketAddress local_address)throws Exception {

        Objects.requireNonNull(media_directory, "media_directory");
        Objects.requireNonNull(local_address, "local_address");

        final String directory =
                media_directory.toAbsolutePath().toString();

        MediaDriver.Context media_ctx =
                new MediaDriver.Context()
                        .dirDeleteOnStart(true)
                        .aeronDirectoryName(directory);

        Aeron.Context aeron_ctx = new Aeron.Context().aeronDirectoryName(directory);
        MediaDriver driver = null;
        Aeron aeron = null;
        try {
            driver = MediaDriver.launch(media_ctx);
            try {
                aeron = Aeron.connect(aeron_ctx);
            } catch (final Exception ex) {
                if (aeron != null) {
                    aeron.close();
                }
                throw ex;
            }
        } catch (final Exception ex) {
            if (driver != null) {
                driver.close();
            }
            throw ex;
        }
        return new EchoServer(driver, aeron, local_address);
    }

    private void runLoop(Subscription sub) throws InterruptedException {
        FragmentHandler handler = new FragmentAssembler(EchoServer::onParseMessage);
        while (true){
            if(sub.isConnected()) {
                sub.poll(handler, 20);
            }
            Thread.sleep(100L);
        }

    }

    private static void onParseMessage(DirectBuffer buffer, int offset, int length, Header header) {
        int sessionId = header.sessionId();
        ServerClient client = clients.get(sessionId);
        if(client == null ){
            LOG.warn("received message from unknown client {}", sessionId);
            return;
        }
        byte[] buf = new byte[length];
        buffer.getBytes(offset, buf);
        final String msg = new String(buf, UTF_8);
        client.onMessageReceived(msg);
    }

    private Subscription setupSubscription(){
        final String sub_uri =
                new ChannelUriStringBuilder()
//                        .reliable(TRUE)
                        .media("udp")
                        .endpoint("localhost:" + local_address.getPort())
                        .build();

        LOG.info("subscription URI: {}", sub_uri);
        return this.aeron.addSubscription(
                sub_uri,
                ECHO_STREAM_ID,
                this::onClientConnected,
                this::onClientDisconnected);
    }

    private void onClientConnected(Image image) {
        int sessionId = image.sessionId();
        LOG.info("onClientConnected: {}", image.sourceIdentity());
        clients.put(sessionId, new ServerClient(sessionId, image, aeron));
    }

    private void onClientDisconnected(Image image) {
        int sessionId = image.sessionId();
        LOG.info("onClientDisconnected: {}", image.sourceIdentity());
        try (final ServerClient client = this.clients.remove(Integer.valueOf(sessionId))) {
            LOG.info("onClientDisconnected: closing client {}", client);
        } catch (final Exception e) {
            LOG.error("onClientDisconnected: failed to close client: ", e);
        }
    }


    @Override
    public void close() throws IOException {
        aeron.close();
        driver.close();
    }
}