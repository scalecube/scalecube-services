import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.rsocket.RSocketJsonPayloadCodec;

import io.rsocket.Payload;

public class Main {
  public static void main(String[] args) {

    RSocketJsonPayloadCodec codec = new RSocketJsonPayloadCodec();
    
    Payload payload = codec.encodeMessage(
        ServiceMessage.builder()
          .header("hello", "world")
          .header("hello1", "world1")
          .data(new MyPojo("ronen"))
          .build());
    
    System.out.println( codec.decodeMessage(payload));
    
  }
}
