package io.scalecube.services.examples;

import com.google.common.util.concurrent.ListenableFuture;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

/**
 * Created by ronenn on 9/11/2016.
        */

@Service
public interface AdSelector {

    @ServiceMethod
    ListenableFuture<Message> select(Message request);

}
