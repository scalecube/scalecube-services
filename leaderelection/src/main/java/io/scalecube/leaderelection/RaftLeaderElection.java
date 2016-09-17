package io.scalecube.leaderelection;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.SimpleTimeLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Raft nodes are always in one of three states: follower, candidate, or leader. All nodes initially start out as a
 * follower. In this state, nodes can cast votes. If no entries are received for some time, nodes self-promote to the
 * candidate state. In the candidate state, nodes request votes from their peers. If a candidate receives a quorum of
 * votes, then it is promoted to a leader. Consensus is fault-tolerant up to the point where quorum is available. If a
 * quorum of nodes is unavailable, it is impossible to reason about peer membership. For example, suppose there are only
 * 2 peers: A and B. The quorum size is also 2. If either A or B fails, it is now impossible to reach quorum. At this
 * point, the algorithm uses random timer timeout and will resolve the situation by one of the nodes taking and
 * maintaining leadership using heartbeats so in this specific case leaders will cancel one another until one of the
 * nodes sends the hearbeats first raft leader election algorithm: when a node starts it becomes a follower and set a timer that will trigger after x
 * amount of time and waiting during this time for a leader heartbeats if no leader send heartbeats then the timer is
 * triggered and node transition to a candidate state - once in a candidate state the node gossip vote request to all
 * member nodes nodes that receive a vote request will answer directly to the requesting node with their vote. the
 * candidate starts to collect the votes and check if there is consensus among the cluster members and that means (N/2
 * +1) votes once the candidate gained consensus it immediately take leadership and start to gossip heartbeats to all
 * cluster nodes. a leader node that receives an heartbeat from any other member transition to follower state in the
 * case where several candidates are running for leadership they both cancel the candidate state and next timeout node
 * will become a leader and election process is restarted until there is clear consensus among the cluster
 * https://raft.github.io/
 * 
 * @author Ronen Nachmias on 9/11/2016.
 */
public class RaftLeaderElection extends RaftStateMachine implements LeaderElection {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private final ICluster cluster;
  private final SimpleTimeLimiter timer = new SimpleTimeLimiter();
  private HeartbeatScheduler heartbeatScheduler;
  private Address selectedLeader;
  private ConcurrentMap<Address, Address> votes = new ConcurrentHashMap();
  private StopWatch leaderHeartbeatTimer;
  private int heartbeatInterval;
  private int leadershipTimeout;
  private List<IStateListener> handlers = new ArrayList();

  public RaftLeaderElection(Builder builder) {
    checkNotNull(builder.cluster);

    this.cluster = builder.cluster;
    this.heartbeatInterval = builder.heartbeatInterval;
    this.leadershipTimeout = builder.leadershipTimeout;
  }

  public static RaftLeaderElection.Builder builder(ICluster cluster) {
    return new RaftLeaderElection.Builder(cluster);
  }

  public RaftLeaderElection start() {
    selectedLeader = cluster.address();

    // RAFT_PROTOCOL_VOTE
    new MessageListener(cluster) {
      @Override
      protected void onMessage(Message message) {
        if (currentState().equals(StateType.CANDIDATE)) {
          votes.putIfAbsent(message.sender(), message.data());
          Address candidate = message.data();
          if (hasConsensus(candidate)) {
            LOGGER.debug(
                "CANDIDATE Node: {} gained Consensus and transition to become leader prev leader was {}",
                cluster.address(), selectedLeader);
            transition(StateType.LEADER);
          }
        }
      }
    }.qualifierEquals(RaftProtocol.RAFT_PROTOCOL_VOTE, MessageListener.ListenType.GOSSIP_OR_TRANSPORT);

    this.heartbeatScheduler = new HeartbeatScheduler(cluster, this.heartbeatInterval);

    // RAFT_PROTOCOL_HEARTBEAT
    new MessageListener(cluster) {
      @Override
      protected void onMessage(Message message) {

        votes.clear();
        leaderHeartbeatTimer.reset();
        selectedLeader = message.data();
        LOGGER.debug("{} Node: {} received heartbeat request from  {}", currentState(), cluster.address(),
            selectedLeader);
        if (currentState().equals(StateType.LEADER) && !(message.data().equals(cluster.address()))){
          transition(StateType.FOLLOWER);
        }
      }
    }.qualifierEquals(RaftProtocol.RAFT_PROTOCOL_HEARTBEAT, MessageListener.ListenType.GOSSIP_OR_TRANSPORT);

    // RAFT_PROTOCOL_REQUEST_VOTE
    new MessageListener(cluster) {
      @Override
      protected void onMessage(Message message) {
        LOGGER.debug("{} Node: {} received vote request from  {}", currentState(), cluster.address(),
            message.data());
        vote(message.data());
      }
    }.qualifierEquals(RaftProtocol.RAFT_PROTOCOL_REQUEST_VOTE, MessageListener.ListenType.GOSSIP_OR_TRANSPORT);

    this.leaderHeartbeatTimer = new StopWatch(leadershipTimeout, TimeUnit.SECONDS) {
      @Override
      public void onTimeout() {
        transition(StateType.CANDIDATE);
      }
    };

    this.leaderHeartbeatTimer.reset();
    transition(StateType.FOLLOWER);
    return this;
  }

  @Override
  public Address leader() {
    return selectedLeader;
  }

  public void addStateListener(IStateListener handler) {
    handlers.add(handler);
  }

  @Override
  protected void becomeFollower() {
    this.heartbeatScheduler.stop();
    leaderHeartbeatTimer.reset();
    votes.clear();
    onStateChanged(StateType.FOLLOWER);
  }

  @Override
  protected void becomeCanidate() {
    selectedLeader = cluster.address();
    requestVote();
    onStateChanged(StateType.CANDIDATE);
  }

  protected void becomeLeader() {
    votes.clear();
    selectedLeader = cluster.address();
    for (Member member : cluster.members()) {
      cluster.send(member.address(),
          Message.builder().qualifier(RaftProtocol.RAFT_PROTOCOL_HEARTBEAT).data(selectedLeader).build());
    }
    this.heartbeatScheduler.schedule();
    onStateChanged(StateType.LEADER);
  }

  private void onStateChanged(StateType state) {
    LOGGER.debug("Node: {} state changed current state {}", cluster.address(), currentState());
    for (IStateListener handler : handlers) {
      handler.onState(state);
    }
  }

  private void requestVote() {
    for (Member member : cluster.otherMembers()) {
      cluster.send(member.address(), Message.builder()
          .qualifier(RaftProtocol.RAFT_PROTOCOL_REQUEST_VOTE)
          .data(cluster.address()).build());
    }
    votes.putIfAbsent(cluster.address(), cluster.address());
  }

  private void vote(Address candidate) {
    // another leader requesting a vote while current node is a leader
    if (currentState().equals(StateType.LEADER)) {
      transition(StateType.FOLLOWER);
      return;
    }
    cluster.send(candidate, Message.builder()
        .qualifier(RaftProtocol.RAFT_PROTOCOL_VOTE)
        .data(candidate)
        .build());
  }

  private boolean hasConsensus(Address candidate) {
    if (candidate.equals(cluster.address())) {
      int consensus = ((cluster.members().size() / 2) + 1);
      return consensus <= votes.size();
    } else {
      return false;
    }
  }

  public static class Builder {
    public ICluster cluster;
    private int heartbeatInterval = 5;
    private int leadershipTimeout = 30;

    public Builder(ICluster cluster) {
      this.cluster = cluster;
    }

    public Builder heartbeatInterval(int heartbeatInterval) {
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    public Builder leadershipTimeout(int leadershipTimeout) {
      this.leadershipTimeout = leadershipTimeout;
      return this;
    }

    public RaftLeaderElection build() {
      return new RaftLeaderElection(this);
    }
  }

  @Override
  public ICluster cluster() {
    return cluster;
  }
}
