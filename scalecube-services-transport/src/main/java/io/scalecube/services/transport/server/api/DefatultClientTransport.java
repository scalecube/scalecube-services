package io.scalecube.services.transport.server.api;

import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

public class DefatultClientTransport implements ClientTransport {

  @Override
  public ClientChannel create(Address address) {
    // TODO Auto-generated method stub
    return null;
  }

  Frame
  1---------0cccccccccccccc
  
  ServiceMessage
  headers
  data
  
  encode(ServiceMessage) {
   Payload(encode(sm,json|proto)) 0-100
   Payload(bytes)      100-500;
  }
  
  headers = JsonDecode(headers)
  
  onMessage
  
}
