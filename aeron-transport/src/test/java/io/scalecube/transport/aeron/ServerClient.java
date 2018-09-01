package io.scalecube.transport.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ServerClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerClient.class);
    private static final Pattern HELLO_PATTERN =  Pattern.compile("HELLO ([0-9]+)");
    private static final int ECHHO_STREAM_ID = 222;

    private final int sessionId;
    private final Image image;
    private final Aeron aeron;
    private final UnsafeBuffer buffer;
    private State state;
    private Publication publication;

    public ServerClient(int sessionId, Image image, Aeron aeron) {
        this.sessionId = sessionId;
        this.image = image;
        this.aeron = aeron;
        this.state = State.INIT;
        this.buffer = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(2048, 16));
    }

    @Override
    public void close()  {
        aeron.close();
    }

    public void onMessageReceived(final String msg) {
        LOG.info(
                "receive [0x{}]: {}",
                Integer.toUnsignedString(this.sessionId),
                msg);
        switch (state){
            case INIT:
                onInitMessage(msg);
                break;
            case PROCESSING:
                sendMessage(msg);
                break;
        }
    }

    private void onInitMessage(String msg) {
        final Matcher matcher = HELLO_PATTERN.matcher(msg);
        if(!matcher.matches()){
            LOG.warn("Client sent malformed init message: {}", msg);
            return;
        }

        try{
            String source_id = image.sourceIdentity();

            String host = new URI("fake://" + source_id).getHost();
            int port = Integer.parseUnsignedInt(matcher.group(1));

            /*final String address =
                    new StringBuilder(64)
                            .append(host)
                            .append(":")
                            .append(port)
                            .toString();*/

            String address = "localhost:" + port;

            final String pub_uri =
                    new ChannelUriStringBuilder()
//                            .reliable(TRUE)
                            .media("udp")
                            .endpoint(address)
                            .build();

            this.publication = aeron.addPublication(pub_uri, ECHHO_STREAM_ID);
            return;

        } catch (Throwable ex){
            LOG.error("Failed to parse source hostname: {}", image.sourceIdentity());
            return;
        }


    }
    private boolean sendMessage(String text) {
        LOG.info("send: {}", text);

        final byte[] value = text.getBytes(UTF_8);
        buffer.putBytes(0, value);

        long result = 0L;
        for (int index = 0; index < 5; ++index) {
            result = publication.offer(buffer, 0, text.length());
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


    private enum State{
            INIT,PROCESSING;
    }
}
