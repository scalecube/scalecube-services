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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Random;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;
public final class EchoClient implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(EchoClient.class);

    private static final int ECHO_STREAM_ID = 0x2044f002;

    private final MediaDriver media_driver;
    private final Aeron aeron;
    private final InetSocketAddress local_address;
    private final InetSocketAddress remote_address;

    private EchoClient(
            final MediaDriver in_media_driver,
            final Aeron in_aeron,
            final InetSocketAddress in_local_address,
            final InetSocketAddress in_remote_address)
    {
        this.media_driver =
                Objects.requireNonNull(in_media_driver, "media_driver");
        this.aeron =
                Objects.requireNonNull(in_aeron, "aeron");
        this.local_address =
                Objects.requireNonNull(in_local_address, "local_address");
        this.remote_address =
                Objects.requireNonNull(in_remote_address, "remote_address");
    }

    /**
     * Create a new client.
     *
     * @param media_directory The directory used for the underlying media driver
     * @param local_address   The local address used by the client
     * @param remote_address  The address of the server to which the client will connect
     *
     * @return A new client
     *
     * @throws Exception On any initialization error
     */

    public static EchoClient create(
            final Path media_directory,
            final InetSocketAddress local_address,
            final InetSocketAddress remote_address)
            throws Exception
    {
        Objects.requireNonNull(media_directory, "media_directory");
        Objects.requireNonNull(local_address, "local_address");
        Objects.requireNonNull(remote_address, "remote_address");

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

            return new EchoClient(media_driver, aeron, local_address, remote_address);
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
            try (final Publication pub = this.setupPublication()) {
                this.runLoop(sub, pub);
            }
        }
    }

    private void runLoop(
            final Subscription sub,
            final Publication pub)
            throws InterruptedException
    {
        final UnsafeBuffer buffer =
                new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

        final Random random = new Random();

        /*
         * Try repeatedly to send an initial HELLO message
         */

        while (true) {
            if (pub.isConnected()) {
                if (sendMessage(pub, buffer, "HELLO " + this.local_address.getPort())) {
                    break;
                }
            }

            Thread.sleep(1000L);
        }

        /*
         * Send an infinite stream of random unsigned integers.
         */

        final FragmentHandler assembler =
                new FragmentAssembler(EchoClient::onParseMessage);

        while (true) {
            if (pub.isConnected()) {
                sendMessage(pub, buffer, Integer.toUnsignedString(random.nextInt()));
            }
            if (sub.isConnected()) {
                sub.poll(assembler, 10);
            }
            Thread.sleep(1000L);
        }
    }

    private static void onParseMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
    {
        final byte[] buf = new byte[length];
        buffer.getBytes(offset, buf);
        final String response = new String(buf, UTF_8);
        LOG.debug("response: {}", response);
    }

    private static boolean sendMessage(
            final Publication pub,
            final UnsafeBuffer buffer,
            final String text)
    {
        LOG.debug("send: {}", text);

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

    private Publication setupPublication()
    {
        final String pub_uri =
                new ChannelUriStringBuilder()
                        .reliable(TRUE)
                        .media("udp")
                        .endpoint(this.remote_address.toString().replaceFirst("^/", ""))
                        .build();

        LOG.debug("publication URI: {}", pub_uri);
        return this.aeron.addPublication(pub_uri, ECHO_STREAM_ID);
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
        return this.aeron.addSubscription(sub_uri, ECHO_STREAM_ID);
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
        if (args.length < 5) {
            LOG.error("usage: directory local-address local-port remote-address remote-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final InetAddress local_name = InetAddress.getByName(args[1]);
        final Integer local_port = Integer.valueOf(args[2]);
        final InetAddress remote_name = InetAddress.getByName(args[3]);
        final Integer remote_port = Integer.valueOf(args[4]);

        final InetSocketAddress local_address =
                new InetSocketAddress(local_name, local_port.intValue());
        final InetSocketAddress remote_address =
                new InetSocketAddress(remote_name, remote_port.intValue());

        try (final EchoClient client = create(directory, local_address, remote_address)) {
            client.run();
        }
    }
}
