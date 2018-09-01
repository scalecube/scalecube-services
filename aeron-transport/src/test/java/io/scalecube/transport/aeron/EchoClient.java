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

import static java.nio.charset.StandardCharsets.UTF_8;

public class EchoClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EchoClient.class);

    private final MediaDriver driver;
    private final Aeron aeron;
    private final InetSocketAddress local_address;
    private final InetSocketAddress remote_address;

    private static final int ECHO_STREAM_ID = 12345;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            LOG.error("usage: directory local-address local-port remote-address remote-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final Integer local_port = Integer.valueOf(args[1]);
        final Integer remote_port = Integer.valueOf(args[2]);

        final InetSocketAddress local_address =
                new InetSocketAddress("127.0.0.1", local_port);
        final InetSocketAddress remote_address =
                new InetSocketAddress("127.0.0.1", remote_port);

        try (final EchoClient client = create(directory, local_address, remote_address)) {
            client.run();
        }
    }

    public void run() {
        try(final Subscription sub = this.createSubscription()){
            try(final Publication pub = this.createPublication()){
                this.runLoop(sub, pub);
            }catch (Exception ex){
                LOG.error("Failed to create publication: ", ex);
            }
        }catch (Exception ex){
            LOG.error("Failed to create subscription: ", ex);
        }
    }

    public EchoClient(MediaDriver driver, Aeron aeron,
                      InetSocketAddress local_address, InetSocketAddress remote_address) {
        this.driver = driver;
        this.aeron = aeron;
        this.local_address = local_address;
        this.remote_address = remote_address;
    }


    /**
     * Initialize Media, and start Aeron client with the same media path.
     *
     * @param media_directory
     * @param local_address
     * @param remote_address
     * @return
     * @throws Exception
     */
    public static EchoClient create(
            final Path media_directory,
            final InetSocketAddress local_address,
            final InetSocketAddress remote_address
            ) throws Exception {

                Objects.requireNonNull(media_directory, "media_directory");
                Objects.requireNonNull(local_address, "local_address");
                Objects.requireNonNull(remote_address, "remote_address");

                final String directory =
                        media_directory.toAbsolutePath().toString();

                MediaDriver.Context media_ctx =

                new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .aeronDirectoryName(directory);

                Aeron.Context aeron_ctx = new Aeron.Context().aeronDirectoryName(directory);
                MediaDriver driver = null;
                Aeron aeron = null;
                try{
                    driver =  MediaDriver.launch(media_ctx);
                    try{
                        aeron = Aeron.connect(aeron_ctx);
                    }catch (final Exception ex){
                        if(aeron != null ){
                            aeron.close();
                        }
                        throw ex;
                    }
                } catch (final Exception ex){
                    if(driver != null){
                        driver.close();
                    }
                    throw ex;
                }
                return new EchoClient(driver, aeron, local_address, remote_address);
    }

    @Override
    public void close() {
        aeron.close();
        driver.close();
    }

    private Subscription createSubscription(){
        final String subscription_channel = new ChannelUriStringBuilder()
//            .reliable(Boolean.TRUE)
            .media("udp")
            .endpoint("localhost:" + local_address.getPort())
            .build();

        LOG.info("Subscription URI: {}", subscription_channel);
        return aeron.addSubscription(subscription_channel, ECHO_STREAM_ID);
    }

    private Publication createPublication(){
        final String publication_channel_uri = new ChannelUriStringBuilder()
//                .reliable(Boolean.TRUE)
                .media("udp")
                .endpoint("localhost:" + remote_address.getPort())
                .build();
        LOG.info("Publication URI: {}", publication_channel_uri);
        return aeron.addPublication(publication_channel_uri, ECHO_STREAM_ID);
    }

    private void runLoop(
            final Subscription subscription,
            final Publication publication) throws InterruptedException {
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));
        Random r = new Random();

        while(true){
//            if(publication.isConnected()){
                if(sendMessage(publication, buffer, "HELLO " + this.local_address.getPort())){
                    break;
                }
                Thread.sleep(1000);
//            }
        }

        FragmentHandler handler = new FragmentAssembler(EchoClient::onParseMessage);
        while(true){
            if(publication.isConnected()){
                sendMessage(publication, buffer, Integer.toUnsignedString(r.nextInt()));
            }
            if(subscription.isConnected()){
                subscription.poll(handler, 10);
            }
            Thread.sleep(1000);
        }
    }

    private static boolean sendMessage(
            final Publication pub,
            final UnsafeBuffer buffer,
            final String text)
    {
        LOG.info("send: {}", text);

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

    private static void onParseMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
    {
        final byte[] buf = new byte[length];
        buffer.getBytes(offset, buf);
        final String response = new String(buf, UTF_8);
        LOG.info("response: {}", response);
    }
}
