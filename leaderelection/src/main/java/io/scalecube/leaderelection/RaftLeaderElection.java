package io.scalecube.leaderelection;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.leaderelection.LeadershipEvent.Type;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
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
public class RaftLeaderElection implements LeaderElection {

  /**
   * State Machine states.
   */
  protected enum StateType {
    START, FOLLOWER, CANDIDATE, LEADER, STOPPED
  }

  public static final String HEARTBEAT = "sc/raft/heartbeat";
  public static final String VOTE_REQUEST = "sc/raft/vote/request";
  public static final String VOTE_RESPONSE = "sc/raft/vote/response";
  private static final String NEW_LEADER_ELECTED = "sc/raft/new/leader/notification";

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);


  private final ICluster cluster;
  private final AtomicReference<StateType> currentState;
  private HeartbeatScheduler heartbeatScheduler;
  private Address selectedLeader;
  private ConcurrentMap<Address, Address> votes = new ConcurrentHashMap<Address, Address>();

  private int heartbeatInterval;
  private int leadershipTimeout;

  private final ScheduledExecutorService executor;
  private AtomicReference<ScheduledFuture<?>> heartbeatTask;
  private Subject<LeadershipEvent, LeadershipEvent> subject;



  /**
   * Builder of raft leader election.
   * 
   * @author Ronen Nachmias
   *
   */
  public static class Builder {
    public ICluster cluster;
    private int heartbeatInterval = 500;
    private int leadershipTimeout = 1500;

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

    this.currentState = new AtomicReference<StateType>(StateType.START);
    heartbeatTask = new AtomicReference<>();
    String nameFormat = "sc-leaderelection-" + cluster.address().toString();
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());

    subject = PublishSubject.<LeadershipEvent>create().toSerialized();
  }

  @Override
  public Observable<LeadershipEvent> listen() {
    return subject.asObservable();
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
          return msg.qualifier().equals(HEARTBEAT);
        }).subscribe(message -> {
          LOGGER.debug("Received RAFT_HEARTBEAT[{}] from {}", HEARTBEAT, message.data());
          onHeartbeatRecived(message);
        });

    /*
     * listen on leadership notifications.
     */
    cluster.listenGossips().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return msg.qualifier().equals(NEW_LEADER_ELECTED);
        }).subscribe(message -> {
          LOGGER.debug("Received new leader notification[{}] from {}", NEW_LEADER_ELECTED, message.data());
          this.subject.onNext(LeadershipEvent.newLeader(message.data()));
          onHeartbeatRecived(message);
        });

    /*
     * listen on vote requests from followers that elect this node as leader to check if node has concensus.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(message -> {
          return VOTE_REQUEST.equals(message.qualifier());
        }).subscribe(this::vote);

    /*
     * listen on vote requests from candidates and replay with a vote.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return VOTE_RESPONSE.equals(msg.qualifier());
        }).subscribe(this::onVoteRecived);

    /*
     * when node joins the cluster the leader send immediate heartbeat and take ownership on the joining node so the
     * node is not waiting for heartbeat request and becomes candidate.
     */
    cluster.listenMembership().observeOn(Schedulers.from(this.executor))
        .subscribe(event -> {
          if (event.isAdded() && currentState().equals(StateType.LEADER)) {
            leaderSendWelcomeMessage();
          }
        });


    transition(StateType.FOLLOWER);

    return this;
  }

  private void leaderSendWelcomeMessage() {
    cluster.spreadGossip(Message.builder()
        .qualifier(HEARTBEAT)
        .data(cluster.address())
        .build());
  }

  private void resetHeartbeatTimeout() {

    cancelHeartbeatTimeout();
    int timeout = randomTimeOut(this.leadershipTimeout);
    ScheduledFuture<?> future = executor.schedule(() -> {
      LOGGER.debug("Time to become Candidate ", "");
      transition(StateType.CANDIDATE);
    }, timeout, TimeUnit.MILLISECONDS);

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
      this.subject.onNext(LeadershipEvent.newLeader(message.data()));
    } else {
      selectedLeader = message.data();
      LOGGER.debug("{} Node: {} received heartbeat from  {}", currentState(), cluster.address(),
          selectedLeader);
      this.resetHeartbeatTimeout();
    }
    transition(StateType.FOLLOWER);
  }

  @Override
  public Address leader() {
    return selectedLeader;
  }

  private void onVoteRecived(Message message) {

    Address candidate = message.data();
    LOGGER.debug("CANDIDATE Member {} reviced vote from {}", cluster.address(), candidate);
    if (!this.cluster.address().equals(candidate)) {
      if (currentState().equals(StateType.CANDIDATE)) {
        votes.putIfAbsent(candidate, candidate);
        if (hasConsensus(candidate)) {
          LOGGER.debug(
              "CANDIDATE Node: {} gained Consensus and transition to become leader prev leader was {}",
              cluster.address(), selectedLeader);
          transition(StateType.LEADER);
        }
      }
    }
  }

  private void becomeFollower() {
    resetHeartbeatTimeout();
    this.heartbeatScheduler.stop();
    votes.clear();
    onStateChanged(StateType.FOLLOWER);
  }

  private void becomeCanidate() {
    waitForVotesUtilTimeout();
    selectedLeader = cluster.address();
    votes.putIfAbsent(cluster.address(), cluster.address());
    requestVotes();
    onStateChanged(StateType.CANDIDATE);
  }

  private ScheduledFuture<?> waitForVotesUtilTimeout() {
    return executor.schedule(
        () -> {
          LOGGER.debug("Candidatation reached timeout {} adter {} secounds", cluster.address(), leadershipTimeout);
          if (currentState().equals(StateType.CANDIDATE)) {
            transition(StateType.FOLLOWER);
          }
        }, leadershipTimeout, TimeUnit.MILLISECONDS);
  }

  protected void becomeLeader() {
    LOGGER.debug("Member: {} become leader current state {} ", cluster.address(), currentState());
    cancelHeartbeatTimeout();
    this.votes.clear();
    this.selectedLeader = cluster.address();
    this.cluster
        .spreadGossip(Message.builder().qualifier(NEW_LEADER_ELECTED).data(selectedLeader).build());
    this.heartbeatScheduler.schedule();
    onStateChanged(StateType.LEADER);

  }

  private void onStateChanged(StateType state) {
    LOGGER.debug("Node: {} state changed current from {} to {}", cluster.address(), currentState(), state);
    if (StateType.LEADER.equals(currentState()) && !StateType.LEADER.equals(state)) {
      subject.onNext(LeadershipEvent.leadershipRevoked(cluster.address()));

    } else if (!StateType.LEADER.equals(currentState()) && StateType.LEADER.equals(state)) {
      subject.onNext(LeadershipEvent.becameLeader(cluster.address()));
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
          .qualifier(VOTE_REQUEST)
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
          .qualifier(VOTE_RESPONSE)
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
      return ThreadLocalRandom.current().nextInt(this.leadershipTimeout / 2, this.leadershipTimeout);
    } else {
      return 1;
    }
  }

  public StateType currentState() {
    return currentState.get();
  }

  /*
   * transition between possible states
   * 
   * @param state - transition to provided state
   */
  private void transition(StateType state) {
    switch (state) {
      case FOLLOWER:
        if (!currentState.get().equals(StateType.FOLLOWER)) {
          becomeFollower();
          currentState.set(StateType.FOLLOWER);
        }
        break;
      case CANDIDATE:
        if (!currentState.get().equals(StateType.CANDIDATE)) {
          becomeCanidate();
          currentState.set(StateType.CANDIDATE);
        }
        break;
      case LEADER:
        if (!currentState.get().equals(StateType.LEADER)) {
          becomeLeader();
          currentState.set(StateType.LEADER);
        }
        break;
      case START: // do nothing for now
        currentState.set(StateType.START);
        break;
      case STOPPED: // do nothing for now
        currentState.set(StateType.STOPPED);
        break;
      default:
        break;
    }
  }
}
