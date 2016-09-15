package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by ronenn on 9/12/2016.
 */

/*
to run the example use the following commands:
start -> this command will initiate seed node
join <index> -> this command will join a node (seed) from the list
list -> will list the current cluster state and leaders selected
kill <index> -> will shutdown a node

 */
public class LeaderElectionExample {

    public static Map<String, ICluster> nodes = new HashMap<>();

    public static void main(String[] args) {

        // create a scanner so we can read the command-line input
        Scanner scanner = new Scanner(System.in);

        boolean notExitCLI = true;
        while (notExitCLI) {

            //  prompt for the user's name
            System.out.print("ScaleCube: ");
            // get their input as a String
            String command = scanner.next();

            if (command.equals("start")) {
                createNode(null);
            } else if (command.startsWith("join")) {
                String index = scanner.next();
                createNode(index);

            } else if (command.startsWith("list")) {
                for (Map.Entry<String, ICluster> entry : nodes.entrySet()) {
                    System.out.println(entry.getKey() + " - "
                            + entry.getValue().address() +" - "+ entry.getValue().leader()
                    );
                }
            } else if (command.startsWith("kill")) {
                String index = scanner.next();
                ICluster nodeA = nodes.get(index);
                nodeA.shutdown();
                nodes.remove(index);
            }

        }
    }

    public static void createNode(String index) {
        ICluster nodeA ;
        if(index ==null)
            nodeA = Cluster.joinAwait();
        else {
            nodeA = Cluster.joinAwait(nodes.get(index).address());
        }

        nodes.put(String.valueOf(nodes.size()), nodeA);
    }
}

