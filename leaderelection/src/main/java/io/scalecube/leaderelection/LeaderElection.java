package io.scalecube.leaderelection;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;

import rx.Observable;


/**
 * Created by ronenn on 9/12/2016.
 */
public interface LeaderElection {

  Address leader();
  
  Observable<LeadershipEvent> listen();
  
  ICluster cluster();
  
}
