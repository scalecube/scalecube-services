package io.scalecube.cluster.leaderelection;

import com.sun.istack.internal.NotNull;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * Created by ronenn on 9/13/2016.
 */
public abstract class MessageListener {

    public final ICluster cluster;

    public MessageListener(ICluster cluster) {
        this.cluster = cluster;
    }

    void qualifierEquals(@NotNull final String qualifier, @NotNull LISTEN_TYPE type) {
        if (type.equals(LISTEN_TYPE.TRANSPORT) || type.equals(LISTEN_TYPE.GOSSIP_OR_TRANSPORT)) {
            this.cluster.listen().filter(new Func1<Message, Boolean>() {
                @Override
                public Boolean call(Message message) {
                    if (message != null && message.qualifier() != null)
                        return message.qualifier().equals(qualifier);
                    else return false;
                }
            }).subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {

                    onMessage(message);
                }
            });
        }
        if (type.equals(LISTEN_TYPE.GOSSIP) || type.equals(LISTEN_TYPE.GOSSIP_OR_TRANSPORT)) {
            this.cluster.listenGossips().filter(new Func1<Message, Boolean>() {
                @Override
                public Boolean call(Message message) {
                    if (message != null && message.qualifier() != null)
                        return message.qualifier().equals(qualifier);
                    else return false;
                }
            }).subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    onMessage(message);
                }
            });
        }
    }

    void qualifierStartsWith(@NotNull final String qualifier, @NotNull LISTEN_TYPE type) {
        if (type.equals(LISTEN_TYPE.TRANSPORT) || type.equals(LISTEN_TYPE.GOSSIP_OR_TRANSPORT)) {
            this.cluster.listen().filter(new Func1<Message, Boolean>() {
                @Override
                public Boolean call(Message message) {
                    if (message != null && message.qualifier() != null)
                        return message.qualifier().startsWith(qualifier);
                    else return false;
                }
            }).subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    onMessage(message);
                }
            });
        }
        if (type.equals(LISTEN_TYPE.GOSSIP) || type.equals(LISTEN_TYPE.GOSSIP_OR_TRANSPORT)) {
            this.cluster.listenGossips().filter(new Func1<Message, Boolean>() {
                @Override
                public Boolean call(Message message) {
                    if (message != null && message.qualifier() != null)
                        return message.qualifier().startsWith(qualifier);
                    else return false;
                }
            }).subscribe(new Action1<Message>() {
                @Override
                public void call(Message message) {
                    onMessage(message);
                }
            });
        }
    }

    protected abstract void onMessage(Message message);


    enum LISTEN_TYPE {
        GOSSIP,
        TRANSPORT,
        GOSSIP_OR_TRANSPORT

    }

}
