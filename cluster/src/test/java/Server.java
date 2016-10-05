import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.TransportConfig;

public class Server {

  
  
  public static void main(String[] args) {
    TransportConfig transportConfig = TransportConfig.builder().port(4100).build(); 
    ClusterConfig cfg = ClusterConfig.builder().transportConfig(transportConfig).build();
    
    // Start seed node
    ICluster seedNode = Cluster.joinAwait(cfg);
    
    seedNode.listen().filter(msg -> {
      return msg.qualifier().equals("/hello/seed");
    }).subscribe(msg -> {
      seedNode.send(msg.sender(), msg);
    });
    
    while(true){
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
