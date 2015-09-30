package io.servicefabric.transport;

import rx.functions.Func1;

/**
 * Static constants for message headers.
 * @author Anton Kharenko
 */
public final class TransportHeaders {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for different message
   * meanings so it will allow to qualify the specific meaning of data.
   */
  public static final String QUALIFIER = "q";

  /**
   * This header is supposed to be used by application in order to correlate request and response messages.
   */
  public static final String CORRELATION_ID = "cid";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose. It is not supposed
   * to be used by application directly and it is subject to changes in future releases.
   */
  public static final String DATA_TYPE = "_type";

  private TransportHeaders() {
    // Do not instantiate
  }

  public static class Filter implements Func1<Message, Boolean> {
    final String qualifier;
    final String correlationId;

    public Filter(String qualifier) {
      this(qualifier, null);
    }

    public Filter(String qualifier, String correlationId) {
      this.qualifier = qualifier;
      this.correlationId = correlationId;
    }

    @Override
    public Boolean call(Message message) {
      boolean q0 = qualifier.equals(message.header(TransportHeaders.QUALIFIER));
      boolean q1 = correlationId == null || correlationId.equals(message.header(TransportHeaders.CORRELATION_ID));
      return q0 && q1;
    }
  }
}
