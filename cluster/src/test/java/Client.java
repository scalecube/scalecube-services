import java.util.concurrent.atomic.AtomicInteger;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

public class Client {

  public static void main(String[] args) {
    AtomicInteger counter = new AtomicInteger(0);
    AtomicInteger rate = new AtomicInteger(0);
    // Start seed node
    ICluster seedNode = Cluster.joinAwait();

    seedNode.listen().filter(msg -> {
      return msg.qualifier().equals("/hello/seed");
    }).subscribe(msg -> {
      seedNode.send(msg.sender(), msg);
      counter.incrementAndGet();
    });



    ICluster node = Cluster.joinAwait(seedNode.address());
    node.listen().filter(msg -> {
      return msg.qualifier().equals("/hello/seed");
    }).subscribe(msg -> {
      seedNode.send(msg.sender(), msg);
      counter.incrementAndGet();
      if (rate.get() < 100000) {
        for (int i = 0; i < 3; i++) {
          node.send(seedNode.address(), Message.builder().qualifier("/hello/seed").build());
        }
      }else{
        node.send(seedNode.address(), Message.builder().qualifier("/hello/seed").build());
      }
    });

    node.send(seedNode.address(), Message.builder().qualifier("/hello/seed").build());

    AtomicInteger last = new AtomicInteger(0);

    while (true)
      try {
        Thread.sleep(1000);
        System.out.println("msg per secound = " + (counter.get() - last.get()));
        
        last.set(counter.get());
        rate.set(counter.get() - last.get());
        last.set(0);
        counter.set(0);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
  }

}
