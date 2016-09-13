package io.scalecube.services.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.leaderelection.IStateListener;
import io.scalecube.services.leaderelection.LeaderElection;
import io.scalecube.services.leaderelection.RaftLeaderElection;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronenn on 9/12/2016.
 */
public class LeaderElectionExample {

    public static void main(String[] args) {

        ICluster nodeA = Cluster.joinAwait();
        ICluster nodeB = Cluster.joinAwait( nodeA.address());
        ICluster nodeC = Cluster.joinAwait(nodeA.address());

        final LeaderElection LE1 =   RaftLeaderElection.builder(nodeA).build();
        final LeaderElection LE2 =   RaftLeaderElection.builder(nodeB).build();
        final LeaderElection LE3 =   RaftLeaderElection.builder(nodeC).build();


        LE1.addStateListener(new IStateListener() {
            @Override
            public void onState(RaftLeaderElection.State state) {
                System.out.println("LE1 State Changed to :"+ state + "  >>>>>> " + " Leader " + LE1.leader());
            }
        });
        LE2.addStateListener(new IStateListener() {
            @Override
            public void onState(RaftLeaderElection.State state) {
                System.out.println("LE2 State Changed to :"+ state + " >>>>>> " + " Leader " + LE2.leader());
            }
        });
        LE3.addStateListener(new IStateListener() {
            @Override
            public void onState(RaftLeaderElection.State state) {
                System.out.println("LE3 State Changed to :"+ state + " >>>>>> " + " Leader " + LE3.leader());
            }
        });

        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(  new Runnable() {
            @Override
            public void run() {
                System.out.println("Leaders: \n" + LE1.leader() + "\n" + LE2.leader() + "\n" + LE3.leader());
            }
        }, 10,10, TimeUnit.SECONDS);

        System.out.println("hello");
    }

}
