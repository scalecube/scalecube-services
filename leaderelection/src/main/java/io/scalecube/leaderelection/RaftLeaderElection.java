package io.scalecube.leaderelection;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft nodes are always in one of three states: follower, candidate, or leader. All nodes initially start out as a
 * follower. In this state, nodes can cast votes. If no entries are received for some time, nodes self-promote to the
 * candidate state. In the candidate state, nodes request votes from their peers. If a candidate receives a quorum of
 * votes, then it is promoted to a leader. Consensus is fault-tolerant up to the point where quorum is available. If a
 * quorum of nodes is unavailable, it is impossible to reason about peer membership. For example, suppose there are only
 * 2 peers: A and B. The quorum size is also 2. If either A or B fails, it is now impossible to reach quorum. At this
 * point, the algorithm uses random timer timeout and will resolve the situation by one of the nodes taking and
 * maintaining leadership using heartbeats so in this specific case leaders will cancel one another until one of the
 * nodes sends the hearbeats first raft leader election algorithm: when a node starts it becomes a follower and set a
 * timer that will trigger after x amount of time and waiting during this time for a leader heartbeats if no leader send
 * heartbeats then the timer is triggered and node transition to a candidate state - once in a candidate state the node
 * gossip vote request to all member nodes nodes that receive a vote request will answer directly to the requesting node
 * with their vote. the candidate starts to collect the votes and check if there is consensus among the cluster members
 * and that means (N/2 +1) votes once the candidate gained consensus it immediately take leadership and start to gossip
 * heartbeats to all cluster nodes. a leader node that receives an heartbeat from any other member transition to
 * follower state in the case where several candidates are running for leadership they both cancel the candidate state
 * and next timeout node will become a leader and election process is restarted until there is clear consensus among the
 * cluster https://raft.github.io/
 * 
 * @author Ronen Nachmias on 9/11/2016.
 */
public class RaftLeaderElection extends RaftStateMachine implements LeaderElection {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private final ICluster cluster;

  private HeartbeatScheduler heartbeatScheduler;
  private Address selectedLeader;
  private ConcurrentMap<Address, Address> votes = new ConcurrentHashMap<Address, Address>();

  private int heartbeatInterval;
  private int leadershipTimeout;
  private final List<IStateListener> handlers = new ArrayList<IStateListener>();
  private final Random rnd;
  private final ScheduledExecutorService executor;
  private AtomicReference<ScheduledFuture<?>> heartbeatTask;

  /**
   * Builder of raft leader election.
   * 
   * @author Ronen Nachmias
   *
   */
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

    /**
     * builds and starts a new RaftLeaderElection.
     * 
     * @return RaftLeaderElection instance
     */
    public RaftLeaderElection build() {
      return new RaftLeaderElection(this).start();
    }
  }

  private RaftLeaderElection(Builder builder) {
    checkNotNull(builder.cluster);

    this.cluster = builder.cluster;
    this.heartbeatInterval = builder.heartbeatInterval;
    this.leadershipTimeout = builder.leadershipTimeout;
    this.rnd = new Random();
    heartbeatTask = new AtomicReference<>();
    String nameFormat = "sc-leaderelection-" + cluster.address().toString();
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
  }

  /**
   * RaftLeaderElection builder.
   * 
   * @param cluster initiate cluster instance must not be null
   * @return initiated RaftLeaderElection
   */
  public static RaftLeaderElection.Builder builder(ICluster cluster) {
    checkNotNull(cluster);
    return new RaftLeaderElection.Builder(cluster);
  }

  private RaftLeaderElection start() {
    selectedLeader = cluster.address();
    this.heartbeatScheduler = new HeartbeatScheduler(cluster, this.heartbeatInterval);

    /*
     * listen on heartbeats from leaders - leaders are sending heartbeats to maintain leadership.
     */
    cluster.listenGossips().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
            return msg.qualifier().equals(RaftProtocol.RAFT_GOSSIP_HEARTBEAT);
          }).subscribe(message -> {
              LOGGER.debug("Received RAFT_HEARTBEAT[{}] from {}", RaftProtocol.RAFT_GOSSIP_HEARTBEAT, message.data());
              onHeartbeatRecived(message);
            });

    /*
     * listen on vote requests from followers that elect this node as leader to check if node has concensus.
     */
    cluster.listen()
        .filter(message -> {
            return RaftProtocol.RAFT_GOSSIP_VOTE_REQUEST.equals(message.qualifier());
          }).subscribe(this::vote);

    /*
     * listen on vote requests from candidates and replay with a vote.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
            return RaftProtocol.RAFT_MESSAGE_VOTE_RESPONSE.equals(msg.qualifier());
          }).subscribe(this::onVoteRecived);

    /*
     * when node joins the cluster the leader send immediate heartbeat and take ownership on the joining node so the
     * node is not waiting for heartbeat request and becomes candidate.
     */
    cluster.listenMembership().subscribe(event -> {
        if (event.isAdded() && currentState().equals(StateType.LEADER)) {
          leaderSendWelcomeMessage(event.member().address());
        }
      });

    transition(StateType.FOLLOWER);

    return this;
  }

  private void leaderSendWelcomeMessage(Address address) {
    cluster.spreadGossip(Message.builder()
        .qualifier(RaftProtocol.RAFT_GOSSIP_HEARTBEAT)
        .data(cluster.address())
        .build());
  }

  private void resetHeartbeatTimeout() {

    cancelHeartbeatTimeout();
    int timeout = randomTimeOut(this.leadershipTimeout);
    ScheduledFuture<?> future = executor.schedule(() -> {
        LOGGER.debug("Time to become Candidate ", "");
        transition(StateType.CANDIDATE);
      }, timeout, TimeUnit.SECONDS);

    heartbeatTask.set(future);
  }

  private boolean cancelHeartbeatTimeout() {
    if (heartbeatTask.get() != null) {
      return heartbeatTask.get().cancel(true);
    } else {
      return false;
    }
  }

  private void onHeartbeatRecived(Message message) {
    if (!cluster.address().equals(message.data()) && currentState().equals(StateType.LEADER)) {
      transition(StateType.FOLLOWER);
    } else {
      selectedLeader = message.data();
      LOGGER.debug("{} Node: {} received heartbeat from  {}", currentState(), cluster.address(),
          selectedLeader);
      this.resetHeartbeatTimeout();
    }
  }

  @Override
  public Address leader() {
    return selectedLeader;
  }

  private void onVoteRecived(Message message) {

    Address candidate = message.data();
    LOGGER.debug("CANDIDATE Member {} reviced vote from {}", cluster.address(), candidate);
    if (!this.cluster.address().equals(candidate)) {
      // if (currentState().equals(StateType.CANDIDATE)) {
      votes.putIfAbsent(candidate, candidate);
      if (hasConsensus(candidate)) {
        LOGGER.debug(
            "CANDIDATE Node: {} gained Consensus and transition to become leader prev leader was {}",
            cluster.address(), selectedLeader);
        transition(StateType.LEADER);
      }
      // }
    }
  }

  /**
   * add state listener to follow state changes of leadership.
   */
  public void addStateListener(IStateListener handler) {
    handlers.add(handler);
  }

  @Override
  protected void becomeFollower() {
    resetHeartbeatTimeout();
    this.heartbeatScheduler.stop();
    votes.clear();
    onStateChanged(StateType.FOLLOWER);
  }

  @Override
  protected void becomeCanidate() {
    waitForVotesUtilTimeout();
    selectedLeader = cluster.address();
    votes.putIfAbsent(cluster.address(), cluster.address());
    requestVotes();
    onStateChanged(StateType.CANDIDATE);
  }

  private void waitForVotesUtilTimeout() {
    executor.schedule(() -> {
        LOGGER.debug("Candidatation reached timeout {} adter {} secounds", cluster.address(), leadershipTimeout);
        if (currentState().equals(StateType.CANDIDATE)) {
          if (cluster.otherMembers().size() == 0) {
            transition(StateType.LEADER);
          } else {
            transition(StateType.FOLLOWER);
          }
        }
      }, leadershipTimeout, TimeUnit.SECONDS);
  }

  protected void becomeLeader() {
    LOGGER.debug("Member: {} become leader current state {} ", cluster.address(), currentState());
    cancelHeartbeatTimeout();
    this.votes.clear();
    this.selectedLeader = cluster.address();
    this.cluster
        .spreadGossip(Message.builder().qualifier(RaftProtocol.RAFT_GOSSIP_HEARTBEAT).data(selectedLeader).build());
    this.heartbeatScheduler.schedule();
    onStateChanged(StateType.LEADER);
  }

  private void onStateChanged(StateType state) {
    LOGGER.debug("Node: {} state changed current from {} to {}", cluster.address(), currentState(), state);
    for (IStateListener handler : handlers) {
      handler.onState(state);
    }
  }

  /**
   * spread gossip for all cluster memebers and request them to vote for this cluster address. member is also voting for
   * itslef.
   */
  private void requestVotes() {
    for (Member m : cluster.otherMembers()) {
      LOGGER.debug("CANDIDATE Member {} request vote from {}", cluster.address(), m.address());
      cluster.send(m, Message.builder()
          .qualifier(RaftProtocol.RAFT_GOSSIP_VOTE_REQUEST)
          .data(cluster.address()).build());
    }
  }

  /**
   * vote for requesting memeber but first check if this member is a leader if yes then become follower else vote for
   * the requesting memeber.
   * 
   * @param message the requesting member message contains Address to whom to vote for.
   */
  private void vote(Message message) {
    Address candidate = message.data();
    if (currentState().equals(StateType.LEADER)) {
      // another leader requesting a vote while current node is a leader
      transition(StateType.FOLLOWER);
      return;
    } else {
      cluster.send(candidate, Message.builder()
          .qualifier(RaftProtocol.RAFT_MESSAGE_VOTE_RESPONSE)
          .data(cluster.address())
          .build());
    }
  }

  private boolean hasConsensus(Address candidate) {
    if (!candidate.equals(cluster.address())) {
      int consensus = ((cluster.members().size() / 2) + 1);
      return consensus <= votes.size();
    } else {
      return false;
    }
  }

  @Override
  public ICluster cluster() {
    return cluster;
  }

  /**
   * RAFT leader election uses random timeout timers to improve the chance of first node in the cluster timeout first.
   * (reducing the chance for several nodes taking leadership at same time).
   * 
   * @param low - low range for timeout.
   * @param high - high range of timeout.
   * @return random number between high and low.
   */
  private int randomTimeOut(int leadershipTimeout) {
    if (leadershipTimeout > 0) {
      int low = this.leadershipTimeout / 2;
      return rnd.nextInt(this.leadershipTimeout - low) + low;
    } else {
      return 1;
    }
  }
}
