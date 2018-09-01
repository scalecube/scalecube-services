package io.scalecube.transport.aeron;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;
/**
 * A mindlessly simple Echo server.
 */

public final class EchoServer implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);

    private static final int ECHO_STREAM_ID = 0x2044f002;

    private final MediaDriver media_driver;
    private final Aeron aeron;
    private final InetSocketAddress local_address;
    private final ConcurrentHashMap<Integer, ServerClient> clients;

    private EchoServer(
            final MediaDriver in_media_driver,
            final Aeron in_aeron,
            final InetSocketAddress in_local_address)
    {
        this.media_driver =
                Objects.requireNonNull(in_media_driver, "media_driver");
        this.aeron =
                Objects.requireNonNull(in_aeron, "aeron");
        this.local_address =
                Objects.requireNonNull(in_local_address, "local_address");

        this.clients = new ConcurrentHashMap<>(32);
    }

    private static final class ServerClient implements AutoCloseable
    {
        private static final Pattern HELLO_PATTERN =
                Pattern.compile("HELLO ([0-9]+)");

        private enum State
        {
            INITIAL,
            CONNECTED
        }

        private final int session;
        private final Image image;
        private final Aeron aeron;
        private State state;
        private final UnsafeBuffer buffer;
        private Publication publication;

        ServerClient(
                final int session,
                final Image in_image,
                final Aeron in_aeron)
        {
            this.session = session;
            this.image = Objects.requireNonNull(in_image, "image");
            this.aeron = Objects.requireNonNull(in_aeron, "aeron");
            this.state = State.INITIAL;
            this.buffer = new UnsafeBuffer(
                    BufferUtil.allocateDirectAligned(2048, 16));
        }

        @Override
        public void close()
                throws Exception
        {
            closeIfNotNull(this.publication);
        }

        public void onReceiveMessage(
                final String message)
        {
            Objects.requireNonNull(message, "message");

            LOG.debug(
                    "receive [0x{}]: {}",
                    Integer.toUnsignedString(this.session),
                    message);

            switch (this.state) {
                case INITIAL: {
                    this.onReceiveMessageInitial(message);
                    break;
                }
                case CONNECTED: {
                    sendMessage(this.publication, this.buffer, message);
                    break;
                }
            }
        }

        private void onReceiveMessageInitial(
                final String message)
        {
            final Matcher matcher = HELLO_PATTERN.matcher(message);
            if (!matcher.matches()) {
                LOG.warn("client sent malformed HELLO message: {}", message);
                return;
            }

            final int port =
                    Integer.parseUnsignedInt(matcher.group(1));
            final String source_id =
                    this.image.sourceIdentity();

            try {
                final URI source_uri =
                        new URI("fake://" + source_id);

                final String address =
                        new StringBuilder(64)
                                .append(source_uri.getHost())
                                .append(":")
                                .append(port)
                                .toString();

                final String pub_uri =
                        new ChannelUriStringBuilder()
                                .reliable(TRUE)
                                .media("udp")
                                .endpoint(address)
                                .build();

                this.publication =
                        this.aeron.addPublication(pub_uri, ECHO_STREAM_ID);

                this.state = State.CONNECTED;
            } catch (final URISyntaxException e) {
                LOG.warn("client sent malformed HELLO message: {}: ", message, e);
            }
        }
    }

    /**
     * Create a new server.
     *
     * @param media_directory The directory used for the underlying media driver
     * @param local_address   The local address used by the server
     *
     * @return A new server
     *
     * @throws Exception On any initialization error
     */

    public static EchoServer create(
            final Path media_directory,
            final InetSocketAddress local_address)
            throws Exception
    {
        Objects.requireNonNull(media_directory, "media_directory");
        Objects.requireNonNull(local_address, "local_address");

        final String directory =
                media_directory.toAbsolutePath().toString();

        final MediaDriver.Context media_context =
                new MediaDriver.Context()
                        .dirDeleteOnStart(true)
                        .aeronDirectoryName(directory);

        final Aeron.Context aeron_context =
                new Aeron.Context().aeronDirectoryName(directory);

        MediaDriver media_driver = null;

        try {
            media_driver = MediaDriver.launch(media_context);

            Aeron aeron = null;
            try {
                aeron = Aeron.connect(aeron_context);
            } catch (final Exception e) {
                closeIfNotNull(aeron);
                throw e;
            }

            return new EchoServer(media_driver, aeron, local_address);
        } catch (final Exception e) {
            closeIfNotNull(media_driver);
            throw e;
        }
    }

    private static void closeIfNotNull(
            final AutoCloseable closeable)
            throws Exception
    {
        if (closeable != null) {
            closeable.close();
        }
    }

    public void run()
            throws Exception
    {
        try (final Subscription sub = this.setupSubscription()) {
            this.runLoop(sub);
        }
    }

    private void runLoop(
            final Subscription sub)
            throws InterruptedException
    {
        final FragmentHandler assembler =
                new FragmentAssembler(this::onParseMessage);

        while (true) {
            if (sub.isConnected()) {
                sub.poll(assembler, 10);
            }

            Thread.sleep(100L);
        }
    }

    private void onParseMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
    {
        final int session = header.sessionId();

        final ServerClient client = this.clients.get(Integer.valueOf(session));
        if (client == null) {
            LOG.warn(
                    "received message from unknown client: {}",
                    Integer.valueOf(session));
            return;
        }

        final byte[] buf = new byte[length];
        buffer.getBytes(offset, buf);
        final String message = new String(buf, UTF_8);
        client.onReceiveMessage(message);
    }

    private static boolean sendMessage(
            final Publication pub,
            final UnsafeBuffer buffer,
            final String text)
    {
        LOG.debug(
                "send: [session 0x{}] {}",
                Integer.toUnsignedString(pub.sessionId()),
                text);

        final byte[] value = text.getBytes(UTF_8);
        buffer.putBytes(0, value);

        long result = 0L;
        for (int index = 0; index < 5; ++index) {
            result = pub.offer(buffer, 0, text.length());
            if (result < 0L) {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            return true;
        }

        LOG.error("could not send: {}", Long.valueOf(result));
        return false;
    }

    private Subscription setupSubscription()
    {
        final String sub_uri =
                new ChannelUriStringBuilder()
                        .reliable(TRUE)
                        .media("udp")
                        .endpoint(this.local_address.toString().replaceFirst("^/", ""))
                        .build();

        LOG.debug("subscription URI: {}", sub_uri);
        return this.aeron.addSubscription(
                sub_uri,
                ECHO_STREAM_ID,
                this::onClientConnected,
                this::onClientDisconnected);
    }

    private void onClientDisconnected(
            final Image image)
    {
        final int session = image.sessionId();
        LOG.debug("onClientDisconnected: {}", image.sourceIdentity());

        try (final ServerClient client = this.clients.remove(Integer.valueOf(session))) {
            LOG.debug("onClientDisconnected: closing client {}", client);
        } catch (final Exception e) {
            LOG.error("onClientDisconnected: failed to close client: ", e);
        }
    }

    private void onClientConnected(
            final Image image)
    {
        final int session = image.sessionId();
        LOG.debug("onClientConnected: {}", image.sourceIdentity());

        this.clients.put(
                Integer.valueOf(session),
                new ServerClient(session, image, this.aeron));
    }

    @Override
    public void close()
    {
        this.aeron.close();
        this.media_driver.close();
    }

    public static void main(
            final String[] args)
            throws Exception
    {
        if (args.length < 3) {
            LOG.error("usage: directory local-address local-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final InetAddress local_name = InetAddress.getByName(args[1]);
        final Integer local_port = Integer.valueOf(args[2]);

        final InetSocketAddress local_address =
                new InetSocketAddress(local_name, local_port.intValue());

        try (final EchoServer server = create(directory, local_address)) {
            server.run();
        }
    }
}