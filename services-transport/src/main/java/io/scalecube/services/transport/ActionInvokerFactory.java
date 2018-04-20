package io.scalecube.services.transport;

import java.net.InetSocketAddress;

public interface ActionInvokerFactory {

  <REQ, RESP> ActionMethodInvoker<REQ, RESP> create(InetSocketAddress address);

}
