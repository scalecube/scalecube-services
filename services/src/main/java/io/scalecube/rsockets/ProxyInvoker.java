package io.scalecube.rsockets;

import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

/**
 * Service interface -> ServiceReference (using ServiceRegistry and Router)
 *
 * Reslove RSocket client (local/remote)
 *
 * Create Action invoker of needed type
 *
 * invoke!
 */
public class ProxyInvoker<SERVICE> {
    final Router router;
    final ServiceRegistry serviceRegistry;

}
