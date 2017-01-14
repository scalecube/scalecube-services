package io.scalecube.services;

import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import java.util.Collection;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class InjectorAnnotationTest {
  
  @Test
  public void service_proxy_constructor_inject_test_with_default_value()
  {
    AnnotationServiceProcessor serviceProcessor  = new AnnotationServiceProcessor();
    Collection<ProxyDefinition> proxyDefList = serviceProcessor
        .extractServiceProxyFromConstructor(CoarseGrainedServiceImpl.class.getConstructors()[1]);
    
    assertTrue(!proxyDefList.isEmpty());
    ProxyDefinition proxyDef = proxyDefList.iterator().next();
    Assert.assertEquals(RoundRobinServiceRouter.class,proxyDef.getRouter());
    Assert.assertEquals(0,proxyDef.getDuration().toMillis());
  }
  
  @Test
  public void service_proxy_constructor_inject_test_with_custom_router()
  {
    AnnotationServiceProcessor serviceProcessor  = new AnnotationServiceProcessor();
    Collection<ProxyDefinition> proxyDefList = serviceProcessor
        .extractServiceProxyFromConstructor(CoarseGrainedConfigurableServiceImpl.class.getConstructors()[1]);
    
    assertTrue(!proxyDefList.isEmpty());
    ProxyDefinition proxyDef = proxyDefList.iterator().next();
    Assert.assertEquals(RoundRobinServiceRouter.class,proxyDef.getRouter());
    Assert.assertEquals(10,proxyDef.getDuration().toMillis());
  }
}
