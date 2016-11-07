package io.scalecube.transport;

/**
 * Static constants for message headers.
 * 
 * @author Anton Kharenko
 */
public final class MessageHeaders {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages
   * so it will allow to qualify the specific message type.
   */
  public static final String QUALIFIER = "q";

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages
   * so it will allow to describe the specific method type.
   */
  public static final String METHOD = "m";
  
  /**
   * This header is supposed to be used by application in order to correlate request and response messages.
   */
  public static final String CORRELATION_ID = "cid";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose. It is not supposed
   * to be used by application directly and it is subject to changes in future releases.
   */
  public static final String DATA_TYPE = "_type";

 

  private MessageHeaders() {
    // Do not instantiate
  }

}
