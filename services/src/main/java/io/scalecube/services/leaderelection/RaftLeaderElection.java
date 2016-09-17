package io.scalecube.services.leaderelection;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import rx.functions.Action1;
import rx.functions.Func1;
import unquietcode.tools.esm.EnumStateMachine;
import unquietcode.tools.esm.StateHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronenn on 9/11/2016.
 *
 * raft leader election algorithm works in the following way
 * when a node starts it becomes a follower and set a timer that will trigger after x amount of time and waiting during this time for a leader heartbeats
 * if no leader send heartbeats then the timer triggers and nodes transition to candidate state - once in candidate state the node gossip vote request to all nodes
 * nodes that recive vote request answer directly to the node with a vote.
 * the node collect the votes and if he has consensus from the cluster (N/2 +1) votes of the cluster the node becomes leader and start to send heartbeats
 *
 * in the cases where there are several candidates running for leadership the last one that will send heartbeats will be come a leader
 *  - it works since the cancel each other until and if leader gets
 * heartbeat from another candidate he becomes a candidate as well and the entire process starts until there is consensus or one of the nodes is the first to take leadership.
 *
 */
public class RaftLeaderElection implements LeaderElection{

    public static final String RAFT_PROTOCOL_HEARTBEAT = "raft.protocol.heartbeat";
    public static final String RAFT_PROTOCOL_REQUEST_VOTE = "raft.protocol.request.vote";
    public static final String RAFT_PROTOCOL_VOTE = "raft.protocol.vote";

    public enum State {
        FOLLOWER, CANDIDATE, LEADER
    }

    /**
     *
     */
    private final ICluster cluster;
    private final SimpleTimeLimiter timer = new SimpleTimeLimiter();
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    private final EnumStateMachine stateMachine;
    private Address selectedLeader;
    private ConcurrentMap<Address, Address> votes = new ConcurrentHashMap();
    private StopWatch leaderHeartbeatTimer;
    private int heartbeatInterval;
    private int leadershipTimeout;

    private List<IStateListener> handlers = new ArrayList();

    public static class Builder {
        public ICluster cluster;
        public int heartbeatInterval = 5;
        public int leadershipTimeout = 30;

        public Builder (ICluster cluster) {
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

    public static RaftLeaderElection.Builder builder(ICluster cluster) {
        return new RaftLeaderElection.Builder(cluster);
    }

    private RaftLeaderElection(Builder builder) {

        this.cluster = builder.cluster;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.leadershipTimeout = builder.leadershipTimeout;
        stateMachine =  initStateMachine();
        selectedLeader = cluster.address();
        //      RAFT_PROTOCOL_VOTE
        new MessageListener(cluster){
            @Override
            protected void onMessage(Message message) {
                if (stateMachine.currentState().equals(State.CANDIDATE)) {
                    votes.putIfAbsent(message.sender(), (Address) message.data());
                    Address candidate = (Address) message.data();
                    if (hasConsensus(candidate)) {
                        stateMachine.transition(State.LEADER);
                    }
                }
            }
        }.qualifierEquals( RAFT_PROTOCOL_VOTE , MessageListener.LISTEN_TYPE.GOSSIP_OR_TRANSPORT);

        //       RAFT_PROTOCOL_HEARTBEAT
        new MessageListener(cluster){
            @Override
            protected void onMessage(Message message) {
                votes.clear();
                leaderHeartbeatTimer.reset();
                selectedLeader = message.data();
                if(stateMachine.currentState().equals(State.LEADER))
                    stateMachine.transition(State.FOLLOWER);
            }
        }.qualifierEquals(RAFT_PROTOCOL_HEARTBEAT , MessageListener.LISTEN_TYPE.GOSSIP_OR_TRANSPORT);

        //     RAFT_PROTOCOL_REQUEST_VOTE
        new MessageListener(cluster ){
            @Override
            protected void onMessage(Message message) {
                vote((Address) message.data());
            }
        }.qualifierEquals(RAFT_PROTOCOL_REQUEST_VOTE, MessageListener.LISTEN_TYPE.GOSSIP_OR_TRANSPORT);

        this.leaderHeartbeatTimer = new StopWatch(leadershipTimeout, TimeUnit.SECONDS) {
            @Override
            public void onTimeout() {
                stateMachine.transition(State.CANDIDATE);
            }
        };

        this.leaderHeartbeatTimer.reset();
    }

    @Override
    public Address leader() {
        return selectedLeader;
    }

    public void addStateListener(IStateListener handler){
        handlers.add(handler);
    }

    private EnumStateMachine initStateMachine() {
        EnumStateMachine<State> esm =  new EnumStateMachine<State>(State.FOLLOWER);
        esm.addTransitions(State.FOLLOWER, State.CANDIDATE);
        esm.addTransitions(State.CANDIDATE, State.LEADER);
        esm.addTransitions(State.LEADER, State.FOLLOWER);

        esm.onEntering(State.FOLLOWER, new StateHandler<State>() {
            public void onState(State state) {
                leaderHeartbeatTimer.reset();
                votes.clear();
                onStateChanged(State.FOLLOWER);
            }
        });

        esm.onEntering(State.CANDIDATE, new StateHandler<State>() {
            public void onState(State state) {
                selectedLeader = cluster.address();
                requestVote();
                onStateChanged(State.CANDIDATE);
            }
        });

        esm.onEntering(State.LEADER, new StateHandler<State>() {
            public void onState(State state) {
                becomeLeader();
                votes.clear();
                onStateChanged(State.LEADER);
            }
        });

        return esm;
    }
    private void onStateChanged(State state){
        for (IStateListener handler : handlers){
            handler.onState(state);
        }
    }
    private void requestVote() {
        for(ClusterMember member :cluster.otherMembers()) {
            cluster.send(member.address(), Message.builder()
                    .qualifier(RAFT_PROTOCOL_REQUEST_VOTE)
                    .data(cluster.address()).build());
        }
        votes.putIfAbsent(cluster.address(), cluster.address());
    }

    private void vote(Address candidate) {
        cluster.send(candidate, Message.builder()
                .qualifier(RAFT_PROTOCOL_VOTE)
                .data(candidate)
                .build());
    }

    private boolean hasConsensus(Address candidate) {
        if (((Address) candidate).equals(cluster.address())) {
            int consensus = ((cluster.members().size() / 2) + 1) ;
            return consensus <= votes.size();
        } else
            return false;
    }


    private void becomeLeader() {
        votes.clear();
        selectedLeader = cluster.address();

        for(ClusterMember member : cluster.members()){
            cluster.send(member.address(),Message.builder().qualifier(RAFT_PROTOCOL_HEARTBEAT).data(selectedLeader).build());
        }

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                leaderHeartbeatTimer.reset();
                cluster.spreadGossip(Message.builder().qualifier(RAFT_PROTOCOL_HEARTBEAT).data(selectedLeader).build());
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
    }
}
