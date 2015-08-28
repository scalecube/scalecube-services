package io.servicefabric.transport;

/** Thrown to indicate that something happened with application message(in class -- {@code msg}) at the given transport. */
public class TransportMessageException extends TransportException {
    private static final long serialVersionUID = 1L;

    private final Object msg;

    public TransportMessageException(ITransportChannel transport, String message, Object msg) {
        super(transport, message);
        this.msg = msg;
    }

    public Object getMsg() {
        return msg;
    }
}
