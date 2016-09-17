package io.scalecube.services.examples;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.scalecube.transport.Message;

/**
 * Created by ronenn on 9/11/2016.
 */
public class SimpleAdSelector implements AdSelector {

    @Override
    public ListenableFuture<Message> select(Message request) {

        // the logic of selecting add comes here
        return Futures.immediateFuture(Message.builder()
                .data(
                        Ad.builder().id("Ad-X2Fc4V51CC")).build()
                );
    }

}
