package io.scalecube.services.examples;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;

import java.util.Collection;
import java.util.List;

/**
 * Created by ronenn on 9/8/2016.
 */
public class ConsulTest {

    public static void main(String[] args) {


        ConsulClient client = new ConsulClient("localhost");

        // KV
        byte[] binaryData = new byte[] {1,2,3,4,5,6,7};
        client.setKVBinaryValue("someKey", binaryData);

        //list known datacenters
        Response<List<String>> response = client.getCatalogDatacenters();
        System.out.println("Datacenters: " + response.getValue());

        // register new service
        NewService newService = new NewService();
        newService.setId("myapp_01");
        newService.setName("myapp");
        newService.setPort(8080);
        client.agentServiceRegister(newService);


        registerService(client);
    }

    public static void registerService(ConsulClient client) {
        // register new service with associated health check
        NewService newService = new NewService();
        newService.setId("myapp_01");
        newService.setName("myapp");
        newService.setPort(8080);

        NewService.Check serviceCheck = new NewService.Check();
        serviceCheck.setScript("/usr/bin/some-check-script");
        serviceCheck.setInterval("10s");
        newService.setCheck(serviceCheck);

        client.agentServiceRegister(newService);
    }

}
