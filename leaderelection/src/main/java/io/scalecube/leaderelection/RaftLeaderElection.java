package io.scalecube.leaderelection;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  private static final String NEW_LEADER_ELECTED = "sc/raft/new/leader/notification";
  private static final String WELCOME = "sc/raft/welcome";
  
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private Member electedLeader;
  
  private final ICluster cluster;
  private final AtomicInteger currentTerm ;
  private final AtomicReference<StateType> currentState;
  
  private ConcurrentMap<String, Member> votes = new ConcurrentHashMap<String, Member>();
  
  private final ScheduledExecutorService executor;
  private AtomicReference<ScheduledFuture<?>> heartbeatTask;
  private ScheduledThreadPoolExecutor heartbeatScheduler = new ScheduledThreadPoolExecutor(1);
  
  private Subject<LeadershipEvent, LeadershipEvent> subject;
  
  private int heartbeatInterval;
  private int electionTimeout;




  /**
   * Builder of raft leader election.
   * 
   * @author Ronen Nachmias
   *
   */
  public static class Builder {
    public ICluster cluster;
    private int heartbeatInterval = 120;
    private int electionTimeout = 300;

    public Builder(ICluster cluster) {
      this.cluster = cluster;
    }

    public Builder heartbeatInterval(int heartbeatInterval) {
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    public Builder leadershipTimeout(int leadershipTimeout) {
      this.electionTimeout = leadershipTimeout;
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
    this.electionTimeout = builder.electionTimeout;
    
    this.currentTerm = new AtomicInteger(0);
    
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
    electedLeader = null;
    /*
     * listen on heartbeats from leaders - leaders are sending heartbeats to maintain leadership.
     */
    cluster.listenGossips().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return msg.qualifier().equals(AppendEntriesRequest.QUALIFIER);
        }).subscribe(message -> {
          LOGGER.debug("Received RAFT_HEARTBEAT[{}] from {}", AppendEntriesRequest.QUALIFIER, message.data());
          onHeartbeatRecived(message.data());
        });
    
    /*
     * listen on welcome heartbeat from leaders - leaders are sending heartbeats to maintain leadership.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return msg.qualifier().equals(WELCOME);
        }).subscribe(message -> {
          LOGGER.debug("Received WELCOME [{}] from {}", WELCOME, message.data());
          onHeartbeatRecived(message.data());
        });

    /*
     * listen on leadership notifications.
     */
    cluster.listenGossips().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return msg.qualifier().equals(NEW_LEADER_ELECTED);
        }).subscribe(message -> {
          if (!cluster.address().equals(message.data()) // i didnt send this
              && !currentState().equals(StateType.LEADER)) { // and i am not a leader
            
            LOGGER.debug("Received new leader notification[{}] from {}", NEW_LEADER_ELECTED, message.data());
            this.subject.onNext(LeadershipEvent.newLeader(message.data()));
            onHeartbeatRecived(message.data());
          }
        });

    /*
     * listen on a request for vote from candidates.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(message -> {
          return VoteRequest.QUALIFIER.equals(message.qualifier());
        }).subscribe(message -> {
          vote(message.data());
        });

    /*
     * listen on vote requests from candidates and replay with a vote.
     */
    cluster.listen().observeOn(Schedulers.from(this.executor))
        .filter(msg -> {
          return VoteResponse.QUALIFIER.equals(msg.qualifier());
        }).subscribe(message->{
          onVoteRecived(message.data());
        });

    /*
     * when node joins the cluster the leader send immediate heartbeat and take ownership on the joining node so the
     * node is not waiting for heartbeat request and becomes candidate.
     */
    cluster.listenMembership().observeOn(Schedulers.from(this.executor))
        .subscribe(event -> {
          if (event.isAdded() && currentState().equals(StateType.LEADER)) {
            leaderSendWelcomeMessage(event.member());
          }
        });


    transition(StateType.FOLLOWER);

    return this;
  }

  /* 
   *       STATE TRANSITION HANDLERS
   */
  
  private void becomeFollower() {
    stopHeartbeating();
    waitForAppendEntriesTimeout();
    onStateChanged(StateType.FOLLOWER);
  }
  
  private void becomeCanidate() {
    stopWaitingAppendEntries();
    stopHeartbeating();
    
    voteForSelf();
    requestVotes(currentTerm.incrementAndGet());
    
    waitForVotesUtilTimeout();
    onStateChanged(StateType.CANDIDATE);
  }
  
  protected void becomeLeader() {
    LOGGER.debug("Member: {} become leader current state {} ", cluster.address(), currentState());
    stopWaitingAppendEntries();
    
    this.votes.clear();
    this.electedLeader = cluster.member();
    this.cluster
        .spreadGossip(Message.builder()
            .qualifier(AppendEntriesRequest.QUALIFIER)
            .data(new AppendEntriesRequest(currentTerm.get(), electedLeader.id()))
            .build());
    
    scheduleHeartbeats();
    onStateChanged(StateType.LEADER);

  }

  /* 
   *       ON RECIVED REQUESTS
   */
  private void onHeartbeatRecived(AppendEntriesRequest req) {
    if(req.term() > currentTerm.get() ){
      currentTerm.set(req.term());
      transition(StateType.FOLLOWER);
      electedLeader = cluster.member( req.leaderId()).get();
      LOGGER.debug("{} Node: {} received heartbeat from  {}", currentState(), cluster.address(),
          electedLeader);
    }
    this.waitForAppendEntriesTimeout();
  }

  private void onVoteRecived(VoteResponse response) {
    Member candidate = cluster.member(response.electorId()).get();
    if (response.granted() && currentState().equals(StateType.CANDIDATE) ) {
      LOGGER.debug("CANDIDATE Member {} reviced vote from {} {}", cluster.address(), candidate,response);
      collectVoteFor(candidate);
      if (hasConsensus(candidate)) {
        LOGGER.debug(
            "CANDIDATE -> LEADER Node: {} gained Consensus and transition to become leader prev leader was {}",
            cluster.address(), electedLeader);
        transition(StateType.LEADER);
      }
    }
  }
  
  /**
   * vote for requesting memeber but first check if this member is a leader if yes then become follower else vote for
   * the requesting memeber.
   * 
   * @param message the requesting member message contains Address to whom to vote for.
   */
  private void vote(VoteRequest vote) {
    if (cluster.member(vote.candidateId()).isPresent()) {
      Member candidate = cluster.member(vote.candidateId()).get();
      cluster.send(candidate, Message.builder()
          .qualifier(VoteResponse.QUALIFIER)
          .data(new VoteResponse(currentTerm.get(),
              (currentTerm.get() < vote.term()),
              cluster.member().id()))
          .build());
    }
  }

  
  private void leaderSendWelcomeMessage(Member toMember) {
    cluster.send(toMember, Message.builder()
        .qualifier(WELCOME)
        .data(new AppendEntriesRequest(currentTerm.get(), electedLeader.id()))
        .build());
  }

  private void waitForAppendEntriesTimeout() {
    stopWaitingAppendEntries();
    
    int timeout = randomTimeOut(this.electionTimeout);
    ScheduledFuture<?> future = executor.schedule(() -> {
      LOGGER.debug("Heartbeat timeout exeeds while waiting for a leader heartbeat after ()ms "
          + "-> tranistion candidate.", timeout);
      
      transition(StateType.CANDIDATE);
    }, timeout, TimeUnit.MILLISECONDS);

    heartbeatTask.set(future);
  }

  private boolean stopWaitingAppendEntries() {
    if (heartbeatTask.get() != null) {
      return heartbeatTask.get().cancel(true);
    } else {
      return false;
    }
  }

  
  @Override
  public Member leader() {
    return electedLeader;
  }

  

  private void collectVoteFor(Member candidate) {
    votes.putIfAbsent(candidate.id(), candidate);
  }

  private void voteForSelf() {
    votes.putIfAbsent(cluster.member().id(), cluster.member());
  }

  private ScheduledFuture<?> waitForVotesUtilTimeout() {
    return executor.schedule(
        () -> {
          LOGGER.debug("Candidatation reached timeout {} adter {} MILLISECONDS", 
              cluster.address(), electionTimeout);
          
          if (cluster.members().size() > 1 && currentState().equals(StateType.CANDIDATE)) {
            LOGGER.debug("cannidate timeout reached - became flower since: cluster size {} "
                + "and cuttent state",
                cluster.members().size(), currentState());
            transition(StateType.FOLLOWER);
          } else if (cluster.members().size() == 1) {
            LOGGER.debug("cannidate timeout reached - became leader since: cluster size {} "
                + "and cuttent state",
                cluster.members().size(), currentState());
            transition(StateType.LEADER);
          }
        }, electionTimeout, TimeUnit.MILLISECONDS);
  }

  
  private void onStateChanged(StateType state) {
    LOGGER.debug("Node: {} state changed current from {} to {}", cluster.address(), currentState(), state);
    if (StateType.LEADER.equals(currentState()) && !StateType.LEADER.equals(state)) {
      subject.onNext(LeadershipEvent.leadershipRevoked(cluster.member()));

    } else if (!StateType.LEADER.equals(currentState()) && StateType.LEADER.equals(state)) {
      subject.onNext(LeadershipEvent.becameLeader(cluster.member()));
    }
  }

  /**
   * spread gossip for all cluster memebers and request them to vote for this cluster address. member is also voting for
   * itslef.
   */
  private void requestVotes(int currentTerm) {
    for (Member m : cluster.otherMembers()) {
      LOGGER.debug("CANDIDATE Member {} request vote from {}", cluster.address(), m.address());
      cluster.send(m, Message.builder()
          .qualifier(VoteRequest.QUALIFIER)
          .data(new VoteRequest(currentTerm, cluster.member().id()))
          .build());
    }
  }

  

  private boolean hasConsensus(Member candidate) {
    if (!cluster.member().equals(candidate)) {
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
      return ThreadLocalRandom.current().nextInt(this.electionTimeout / 2, this.electionTimeout);
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
  
  
  /**
   * when becoming a leader the leader schedule heatbeats and maintain leadership followers are expecting this heartbeat
   * in case the heartbeat will not arrive in X time they will assume no leader.
   */
  public void scheduleHeartbeats() {
    heartbeatScheduler.scheduleAtFixedRate(this::spreadHeartbeat, 
        heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
  }
  
  private void spreadHeartbeat() {
    LOGGER.debug("Leader Node: {} maintain leadership spread {} gossip regards selected leader {}",
        cluster.address(), AppendEntriesRequest.QUALIFIER, cluster.address());
    
      cluster.spreadGossip(Message.builder()
          .qualifier(AppendEntriesRequest.QUALIFIER)
          .data(new AppendEntriesRequest(currentTerm.get(), cluster.member().id()))
          .build());
    
  }

  /**
   * stop sending heartbeats - it actually means this member is no longer a leader.
   */
  public void stopHeartbeating() {
    heartbeatScheduler.getQueue().clear();
  }

}
