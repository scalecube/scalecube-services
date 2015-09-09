package io.servicefabric.transport;

import rx.functions.Func1;

/**
 * @author Anton Kharenko
 */
public final class TransportHeaders {

  public final static String QUALIFIER = "q";

  public final static String CORRELATION_ID = "cid";

  private TransportHeaders() {
    // Do not instantiate
  }

  public static class Filter implements Func1<TransportMessage, Boolean> {
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
    public Boolean call(TransportMessage transportMessage) {
      boolean q0 = qualifier.equals(transportMessage.message().header(TransportHeaders.QUALIFIER));
      boolean q1 = correlationId == null || correlationId.equals(transportMessage.message().header(TransportHeaders.CORRELATION_ID));
      return q0 && q1;
    }
  }
}
