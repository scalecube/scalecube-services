package io.scalecube.services.state;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;
import io.scalecube.transport.TransportHeaders;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class provides implementation of distributed cluster state maintenance algorithm. It's spreading state updates
 * and synchronizing state between the members of the cluster. It's not a generic state maintenance algorithm, but
 * designed in order to support set of specific use cases and has properties which doesn't fit other use cases. It
 * disseminate state updates over Gossip Protocol and provides additional state synchronization messaging over TCP
 * transport between cluster members in order to init local state and recover from message loss and network
 * partitioning.
 *
 * <p>
 * Algorithm properties:
 * <ul>
 * <li>It doesn't have any centralized coordination and cluster state is maintained by running following algorithm on
 * each member of the cluster.</li>
 * <li>Only current member allowed to update it's local state and it can only listen for state changes on other members.
 * </li>
 * <li>All member state updates are full state update. It doesn't support partial update of member state.</li>
 * <li>Each member stores full replica of latest known state of each cluster members in memory. All state read
 * operations performed over this replica and never involves network calls. State is updated based on event-driven model
 * via messaging.</li>
 * <li>When cluster member is removed from the cluster its state also removed with corresponding event emitted.</li>
 * <li>State update messages spread through the cluster in an eventual consistent way.</li>
 * <li>Only latest state is used. In case if you receive state update which is older then the one you have already it
 * will be ignored so the sequence of updates may be different on different cluster members, but you guarantee to keep
 * latest known state.</li>
 * </ul>
 * 
 * <p>
 * So it provides possibility to synchronize small pieces of information between cluster members. It doesn't fit well
 * for the cases when state of particular member is big or overall cluster state consume a lot of member. Also eventual
 * consistency model should be allowable for the specific use case.
 * 
 * <p>
 * Some use cases which makes good fit for the given algorithm includes maintenance of distributed cluster
 * configuration, service registration, topic subscriptions, group maintenance etc.
 *
 * @param <S> State object class. This object should be immutable otherwise state maintenance algorithm won't work
 *        correctly.
 * 
 * @author Anton Kharenko
 * @see MemberState
 * @see StateEvent
 */
public final class DistributedState<S> {

  // TODO [AK]: Unit and integration tests
  // TODO [AK]: Introduce status and check status transitions (e.g. created->starting->started->...)

  // Message qualifiers
  private static final String Q_STATE_SYNC_REQ = "io.scalecube.state/syncReq";
  private static final String Q_STATE_SYNC_ACK = "io.scalecube.state/syncAck";
  private static final String Q_STATE_UPDATE = "io.scalecube.state/update";

  // Message headers
  private static final String H_STATE_ID = "state:id";
  private static final String H_MEMBER_ID = "state:mid";
  private static final String H_COUNTER = "state:c";

  // Message filters by qualifiers
  private static final Func1<Message, Boolean> SYNC_REQ_MSG_FILTER = new TransportHeaders.Filter(Q_STATE_SYNC_REQ);
  private static final Func1<Message, Boolean> SYNC_ACK_MSG_FILTER = new TransportHeaders.Filter(Q_STATE_SYNC_ACK);
  private static final Func1<Message, Boolean> STATE_UPDATE_MSG_FILTER = new TransportHeaders.Filter(Q_STATE_UPDATE);

  // Subscribers
  private final Subscriber<Message> onStateUpdateSubscriber = createOnStateUpdateSubscriber();
  private final Subscriber<Message> onStateSyncReqSubscriber = createOnStateSyncReqSubscriber();
  private final Subscriber<Message> onStateSyncAckSubscriber = createOnStateSyncAckSubscriber();
  private final Subscriber<ClusterMember> onMembershipUpdateSubscriber = createOnMembershipUpdateSubscriber();

  // Publish subject
  @SuppressWarnings("unchecked")
  private final Subject<StateEvent<S>, StateEvent<S>> stateEventsSubject = new SerializedSubject(
      PublishSubject.create());

  // Instance variables
  private final String stateId;
  private final ICluster cluster;
  private final ConcurrentMap<String, MemberState<S>> clusterState = new ConcurrentHashMap<>();

  /**
   * Creates new instance of a distributed state maintenance component with the given id and initial local state which
   * is synchronizing state between a members of a given cluster.
   *
   * @param stateId identifier of a specific distributed state instance should be unique per given cluster and state.
   * @param cluster cluster in which state should be synchronized.
   * @param initialLocalState initial value of local part of the distributed state.
   */
  public DistributedState(String stateId, ICluster cluster, S initialLocalState) {
    checkArgument(cluster != null);
    checkArgument(stateId != null);
    checkArgument(initialLocalState != null);
    this.stateId = stateId;
    this.cluster = cluster;
    clusterState.put(cluster.membership().localMember().id(),
        MemberState.newInstance(stateId, cluster.membership().localMember().id(), initialLocalState));
  }

  /**
   * Starts running state synchronization algorithm at current member for the given instance of {@code DistributedState}
   * . Only after it was started it will receive and publish updates of distributed cluster state.
   */
  public void start() {
    // Create message filter which check state header match
    final Func1<Message, Boolean> stateIdFilter = createStateIdFilter();

    // Subscribe on state update gossips
    cluster.gossip().listen().filter(stateIdFilter).filter(STATE_UPDATE_MSG_FILTER).subscribe(onStateUpdateSubscriber);

    // Subscribe on membership update events
    cluster.membership().listenUpdates().subscribe(onMembershipUpdateSubscriber);

    // Subscribe on state sync request messages
    cluster.listen().filter(stateIdFilter).filter(SYNC_REQ_MSG_FILTER).subscribe(onStateSyncReqSubscriber);

    // Subscribe on state sync ack messages
    cluster.listen().filter(stateIdFilter).filter(SYNC_ACK_MSG_FILTER).subscribe(onStateSyncAckSubscriber);

    // Sync state with all other active members
    List<ClusterMember> remoteMembers = cluster.membership().members();
    remoteMembers.remove(cluster.membership().localMember());
    for (ClusterMember member : remoteMembers) {
      if (member.status() == ClusterMemberStatus.TRUSTED) {
        sendStateSyncReq(member);
      }
    }
  }

  /**
   * Stops running state synchronization algorithm at current member for the given instance of {@code DistributedState}.
   */
  public void stop() {
    // Unsubscribe from all event/message listeners
    onMembershipUpdateSubscriber.unsubscribe();
    onStateSyncAckSubscriber.unsubscribe();
    onStateSyncReqSubscriber.unsubscribe();
    onStateUpdateSubscriber.unsubscribe();

    // Complete state events subject
    stateEventsSubject.onCompleted();
  }

  /**
   * Sets local state to the given value. This method doesn't check previous value and can cause missed update
   * behaviour. For CAS version of state update see {@link #compareAndSetLocalState(MemberState, Object)}.
   * 
   * @param state given state to set
   */
  public void selLocalState(S state) {
    checkArgument(state != null);
    boolean succeed;
    MemberState<S> newState;
    do {
      MemberState<S> oldState = clusterState.get(cluster.membership().localMember().id());
      newState = oldState.nextState(state);
      succeed = clusterState.replace(cluster.membership().localMember().id(), oldState, newState);
    } while (!succeed);

    spreadStateUpdate();
  }

  /**
   * Updates local state to the given value if previous state equals to the provided one.
   * 
   * @param expectedState expected previous state
   * @param state given state to set
   * @return True if updated successfully; false if expected state doesn't match with actual current state.
   */
  public boolean compareAndSetLocalState(MemberState<S> expectedState, S state) {
    checkArgument(state != null);
    checkArgument(expectedState != null);
    MemberState<S> newState = expectedState.nextState(state);
    boolean succeed = clusterState.replace(cluster.membership().localMember().id(), expectedState, newState);
    if (succeed) {
      spreadStateUpdate();
    }
    return succeed;
  }

  /**
   * Returns current local member state.
   */
  public MemberState<S> getLocalState() {
    return getMemberState(cluster.membership().localMember().id());
  }

  /**
   * Returns state of member by given member id or null if such member doesn't set any state for the given instance of
   * {@code DistributedState}.
   */
  public MemberState<S> getMemberState(String memberId) {
    return clusterState.get(memberId);
  }

  /**
   * Returns map which contains all cluster member states by member id key.
   */
  public Map<String, MemberState<S>> getClusterState() {
    return Collections.unmodifiableMap(clusterState);
  }

  /**
   * Returns stream of {@code StateEvent} which describe state changes received from other cluster members. It doesn't
   * contain events corresponding to local state changes. State changes are eventually consistent and can be received in
   * any order, but always used latest state, so intermediate state events can be missed in case if they arrive later
   * then more recent state update.
   *
   * @return Observable which emit state changed events or complete event when state is stopped.
   */
  public Observable<StateEvent<S>> listenChanges() {
    return stateEventsSubject;
  }

  private Func1<Message, Boolean> createStateIdFilter() {
    return new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        String stateNameHeader = message.header(H_STATE_ID);
        return stateNameHeader != null && stateNameHeader.equals(stateId);
      }
    };
  }

  private Subscriber<Message> createOnStateUpdateSubscriber() {
    return Subscribers.create(new Action1<Message>() {
      @Override
      public void call(Message message) {
        onStateUpdate(message);
      }
    });
  }

  private Subscriber<Message> createOnStateSyncReqSubscriber() {
    return Subscribers.create(new Action1<Message>() {
      @Override
      public void call(Message message) {
        onStateSyncReq(message);
      }
    });
  }

  private Subscriber<Message> createOnStateSyncAckSubscriber() {
    return Subscribers.create(new Action1<Message>() {
      @Override
      public void call(Message message) {
        onStateSyncAck(message);
      }
    });
  }

  // TODO [AK]: Consider to split into two subscribers
  private Subscriber<ClusterMember> createOnMembershipUpdateSubscriber() {
    return Subscribers.create(new Action1<ClusterMember>() {
      @Override
      public void call(ClusterMember member) {
        if (member.status() == ClusterMemberStatus.REMOVED) {
          onMemberRemoved(member);
        } else if (member.status() == ClusterMemberStatus.TRUSTED) {
          // TODO [AK]: Needed only on REACHABLE event (see https://github.com/servicefabric/servicefabric/issues/29)
          onMemberReachable(member);
        }
      }
    });
  }

  private void onMemberRemoved(ClusterMember member) {
    MemberState<S> removedState = clusterState.remove(member.id());
    if (removedState != null) {
      StateEvent<S> event = StateEvent.createRemovedEvent(member.id(), removedState.state());
      stateEventsSubject.onNext(event);
    }
  }

  private void onMemberReachable(ClusterMember member) {
    // Sync states with reachable member
    sendStateSyncReq(member);
  }

  private void onStateSyncReq(Message message) {
    MemberState<S> remoteState = toPartialState(message);
    tryUpdateState(remoteState);
    sendStateSyncAck(cluster.membership().member(remoteState.memberId()));
  }

  private void onStateUpdate(Message message) {
    MemberState<S> remoteState = toPartialState(message);
    tryUpdateState(remoteState);
  }

  private void onStateSyncAck(Message message) {
    MemberState<S> remoteState = toPartialState(message);
    tryUpdateState(remoteState);
  }

  private void sendStateSyncReq(ClusterMember to) {
    cluster.send(to, newStateMessage(Q_STATE_SYNC_REQ));
  }

  private void spreadStateUpdate() {
    cluster.gossip().spread(newStateMessage(Q_STATE_UPDATE));
  }

  private void sendStateSyncAck(ClusterMember to) {
    cluster.send(to, newStateMessage(Q_STATE_SYNC_ACK));
  }

  private MemberState<S> toPartialState(Message message) {
    String memberId = message.header(H_MEMBER_ID);
    long counter = Long.valueOf(message.header(H_COUNTER));
    S state = message.data();
    return MemberState.newInstance(stateId, memberId, state, counter);
  }

  private Message newStateMessage(String qualifier) {
    MemberState<S> localState = clusterState.get(cluster.membership().localMember().id());
    return new Message(localState.state(), TransportHeaders.QUALIFIER, qualifier, H_STATE_ID, stateId, H_MEMBER_ID,
        localState.memberId(), H_COUNTER, Long.toString(localState.counter()));
  }

  private void tryUpdateState(MemberState<S> newMemberState) {
    final String memberId = newMemberState.memberId();
    boolean succeed;
    boolean updated;
    MemberState<S> oldMemberState;
    do {
      if (!clusterState.containsKey(memberId)) {
        oldMemberState = clusterState.putIfAbsent(memberId, newMemberState);
        succeed = updated = (oldMemberState == null);
      } else {
        oldMemberState = clusterState.get(memberId);
        if (newMemberState.counter() > oldMemberState.counter()) {
          succeed = updated = clusterState.replace(memberId, oldMemberState, newMemberState);
        } else {
          succeed = true;
          updated = false;
        }
      }
    } while (!succeed);

    if (updated) {
      StateEvent<S> event =
          oldMemberState == null ? StateEvent.createAddedEvent(memberId, newMemberState.state()) : StateEvent
              .createUpdatedEvent(memberId, oldMemberState.state(), newMemberState.state());

      stateEventsSubject.onNext(event);
    }
  }

}
