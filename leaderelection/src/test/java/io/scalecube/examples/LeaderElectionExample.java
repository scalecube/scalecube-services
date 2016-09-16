package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.leaderelection.IStateListener;
import io.scalecube.leaderelection.LeaderElection;
import io.scalecube.leaderelection.RaftLeaderElection;
import io.scalecube.leaderelection.StateType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by ronenn on 9/12/2016.
 */

/*
 * to run the example use the following commands: start -> this command will initiate seed node join <index> -> this
 * command will join a node (seed) from the list list -> will list the current cluster state and leaders selected kill
 * <index> -> will shutdown a node
 * 
 */
public class LeaderElectionExample {

  public static List<ICluster> nodes = new ArrayList<>();
  public static Map<ICluster, LeaderElection> leaders = new HashMap<>();

  public static void main(String[] args) {

    System.out.println("to run the example use the following commands: "
        + "start -> this command will initiate "
        + "seed node join <index> -> this command will join a node (seed) from the list list -> "
        + "will list the current cluster state and leaders selected kill<index> -> "
        + "will shutdown a node");

    // create a scanner so we can read the command-line input
    Scanner scanner = new Scanner(System.in);

    boolean notExitCLI = true;
    while (notExitCLI) {

      // prompt for the user's name
      System.out.print("ScaleCube: ");
      // get their input as a String
      String command = scanner.next();

      if (command.equals("start")) {
        createNode(-1);
        list();
      } else if (command.startsWith("join")) {
        String index = scanner.next();
        int i=-2;
        try{
          i = Integer.valueOf(index);
        }catch(NumberFormatException ex){
          System.out.println("invalid input value must be valid index number");
        };
        if(i>=0 && i<nodes.size()){
          createNode(i);
          list();
        }

      } else if (command.startsWith("list")) {
        list();
      } else if (command.startsWith("kill")) {
        String index = scanner.next();
        int i = Integer.valueOf(index);
        ICluster nodeA = nodes.get(i);
        nodeA.shutdown();
        nodes.remove(i);
        list();
      }
    }
  }

  private static void list() {
    System.out.println(">>>>>> Cluster Members and leaders <<<<<<");

    for (int i = 0; i < nodes.size(); i++) {
      ICluster entry = nodes.get(i);
      System.out.println(i + " - "
          + entry.address() + " leader: " + leaders.get(entry).leader());
    }
    System.out.println(">>>>>> --------------------------- <<<<<<");

  }

  public static void createNode(int index) {
    ICluster node = null;
    LeaderElection el;

    if (index == -1 && index < nodes.size())
      node = Cluster.joinAwait();
    else {
      if (index >= 0) {
        node = Cluster.joinAwait(nodes.get(index).address());
      }

    }
    el = RaftLeaderElection.builder(node).build().start();
    el.addStateListener(new IStateListener() {
      @Override
      public void onState(StateType state) {
        System.out.println(">>> NODE STATE CHANGED <<<<");
        list();
      }
    });

    nodes.add(node);
    leaders.put(node, el);
  }
}
