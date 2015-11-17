package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.scalecube.cluster.fdetector.FailureDetectorEvent.suspected;
import static io.scalecube.cluster.fdetector.FailureDetectorEvent.trusted;
import static java.lang.Math.min;

import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.TransportHeaders;
import io.scalecube.transport.TransportEndpoint;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FailureDetector implements IFailureDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);

  // qualifiers
  private static final String PING = "io.scalecube.cluster/fdetector/ping";
  private static final String PING_REQ = "io.scalecube.cluster/fdetector/pingReq";
  private static final String ACK = "io.scalecube.cluster/fdetector/ack";

  // filters
  private static final TransportHeaders.Filter ACK_FILTER = new TransportHeaders.Filter(ACK);
  private static final TransportHeaders.Filter PING_FILTER = new TransportHeaders.Filter(PING);
  private static final TransportHeaders.Filter PING_REQ_FILTER = new TransportHeaders.Filter(PING_REQ);

  private volatile List<TransportEndpoint> members = new ArrayList<>();
  private ITransport transport;
  private final TransportEndpoint localEndpoint;
  private final Scheduler scheduler;
  @SuppressWarnings("unchecked")
  private Subject<FailureDetectorEvent, FailureDetectorEvent> subject = new SerializedSubject(PublishSubject.create());
  private AtomicInteger periodNbr = new AtomicInteger();
  private Set<TransportEndpoint> suspectedMembers = Sets.newConcurrentHashSet();
  private TransportEndpoint pingMember; // for test purpose only
  private List<TransportEndpoint> randomMembers; // for test purpose only
  private int pingTime = 2000;
  private int pingTimeout = 1000;
  private int maxEndpointsToSelect = 3;
  private volatile Subscription fdTask;

  /** Listener to PING message and answer with ACK. */
  private Subscriber<Message> onPingSubscriber = Subscribers.create(new Action1<Message>() {
    @Override
    public void call(Message message) {
      LOGGER.trace("Received Ping: {}", message);
      FailureDetectorData data = message.data();
      String correlationId = message.header(TransportHeaders.CORRELATION_ID);
      send(data.getFrom(), new Message(data, TransportHeaders.QUALIFIER, ACK, TransportHeaders.CORRELATION_ID,
          correlationId));
    }
  });

  /** Listener to PING_REQ message and send PING to requested cluster member. */
  private Subscriber<Message> onPingReqSubscriber = Subscribers.create(new Action1<Message>() {
    @Override
    public void call(Message message) {
      LOGGER.trace("Received PingReq: {}", message);
      FailureDetectorData data = message.data();
      TransportEndpoint target = data.getTo();
      TransportEndpoint originalIssuer = data.getFrom();
      String correlationId = message.header(TransportHeaders.CORRELATION_ID);
      FailureDetectorData pingReqData = new FailureDetectorData(localEndpoint, target, originalIssuer);
      send(target, new Message(pingReqData, TransportHeaders.QUALIFIER, PING, TransportHeaders.CORRELATION_ID,
          correlationId));
    }
  });

  /**
   * Listener to ACK with message containing ORIGINAL_ISSUER then convert message to plain ACK and send it to
   * ORIGINAL_ISSUER.
   */
  private Subscriber<Message> onAckToOriginalAckSubscriber = Subscribers
      .create(new Action1<Message>() {
        @Override
        public void call(Message message) {
          FailureDetectorData data = message.data();
          TransportEndpoint target = data.getOriginalIssuer();
          String correlationId = message.header(TransportHeaders.CORRELATION_ID);
          FailureDetectorData originalAckData = new FailureDetectorData(target, data.getTo());
          Message originalAckMessage =
              new Message(originalAckData, TransportHeaders.QUALIFIER, ACK, TransportHeaders.CORRELATION_ID,
                          correlationId);
          send(target, originalAckMessage);
        }
      });

  public FailureDetector(TransportEndpoint localEndpoint, Scheduler scheduler) {
    checkArgument(localEndpoint != null);
    checkArgument(scheduler != null);
    this.localEndpoint = localEndpoint;
    this.scheduler = scheduler;
  }

  public void setPingTime(int pingTime) {
    this.pingTime = pingTime;
  }

  public void setPingTimeout(int pingTimeout) {
    this.pingTimeout = pingTimeout;
  }

  public void setMaxEndpointsToSelect(int maxEndpointsToSelect) {
    this.maxEndpointsToSelect = maxEndpointsToSelect;
  }

  @Override
  public void setClusterEndpoints(Collection<TransportEndpoint> members) {
    Set<TransportEndpoint> set = new HashSet<>(members);
    set.remove(localEndpoint);
    List<TransportEndpoint> list = new ArrayList<>(set);
    Collections.shuffle(list);
    this.members = list;
    LOGGER.debug("Set cluster members: {}", this.members);
  }

  public void setTransport(ITransport transport) {
    this.transport = transport;
  }

  public ITransport getTransport() {
    return transport;
  }

  public TransportEndpoint getLocalEndpoint() {
    return localEndpoint;
  }

  public List<TransportEndpoint> getSuspectedMembers() {
    return new ArrayList<>(suspectedMembers);
  }

  /** <b>NOTE:</b> this method is for test purpose only. */
  void setPingMember(TransportEndpoint member) {
    checkNotNull(member);
    checkArgument(member != localEndpoint);
    this.pingMember = member;
  }

  /** <b>NOTE:</b> this method is for test purpose only. */
  void setRandomMembers(List<TransportEndpoint> randomMembers) {
    checkNotNull(randomMembers);
    this.randomMembers = randomMembers;
  }

  @Override
  public void start() {
    transport.listen().filter(PING_FILTER).filter(targetFilter(localEndpoint)).subscribe(onPingSubscriber);
    transport.listen().filter(PING_REQ_FILTER).subscribe(onPingReqSubscriber);
    transport.listen().filter(ACK_FILTER).filter(new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        FailureDetectorData data = message.data();
        return data.getOriginalIssuer() != null;
      }
    }).subscribe(onAckToOriginalAckSubscriber);

    fdTask = scheduler.createWorker().schedulePeriodically(new Action0() {
      @Override
      public void call() {
        try {
          doPing(members);
        } catch (Exception e) {
          LOGGER.error("Unhandled exception: {}", e, e);
        }
      }
    }, 0, pingTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    if (fdTask != null) {
      fdTask.unsubscribe();
    }
    subject.onCompleted();
    onPingReqSubscriber.unsubscribe();
    onPingReqSubscriber.unsubscribe();
    onAckToOriginalAckSubscriber.unsubscribe();
  }

  @Override
  public Observable<FailureDetectorEvent> listenStatus() {
    return subject;
  }

  @Override
  public void suspect(TransportEndpoint member) {
    checkNotNull(member);
    suspectedMembers.add(member);
  }

  @Override
  public void trust(TransportEndpoint member) {
    checkNotNull(member);
    suspectedMembers.remove(member);
  }

  private void doPing(final List<TransportEndpoint> members) {
    final TransportEndpoint pingMember = selectPingMember(members);
    if (pingMember == null) {
      return;
    }

    final String period = "" + periodNbr.incrementAndGet();
    FailureDetectorData pingData = new FailureDetectorData(localEndpoint, pingMember);
    Message message = new Message(pingData, TransportHeaders.QUALIFIER, PING,
                                  TransportHeaders.CORRELATION_ID, period/* correlationId */);
    LOGGER.trace("Send Ping from {} to {}", localEndpoint, pingMember);

    transport.listen().filter(ackFilter(period)).filter(new CorrelationFilter(localEndpoint, pingMember)).take(1)
        .timeout(pingTimeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(Subscribers.create(new Action1<Message>() {
          @Override
          public void call(Message transportMessage) {
            LOGGER.trace("Received PingAck from {}", pingMember);
            declareTrusted(pingMember);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            LOGGER.trace("No PingAck from {} within {}ms; about to make PingReq now", pingMember, pingTimeout);
            doPingReq(members, pingMember, period);
          }
        }));

    send(pingMember, message);
  }

  private void doPingReq(List<TransportEndpoint> members, final TransportEndpoint targetMember, String period) {
    final int timeout = pingTime - pingTimeout;
    if (timeout <= 0) {
      LOGGER.trace("No PingReq occurred, because no time left (pingTime={}, pingTimeout={})", pingTime, pingTimeout);
      declareSuspected(targetMember);
      return;
    }

    final List<TransportEndpoint> randomMembers =
        selectRandomMembers(members, maxEndpointsToSelect, targetMember/* exclude */);
    if (randomMembers.isEmpty()) {
      LOGGER.trace("No PingReq occurred, because member selection is empty");
      declareSuspected(targetMember);
      return;
    }

    transport.listen().filter(ackFilter(period)).filter(new CorrelationFilter(localEndpoint, targetMember)).take(1)
        .timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(Subscribers.create(new Action1<Message>() {
          @Override
          public void call(Message message) {
            LOGGER.trace("PingReq OK (pinger={}, target={})", message.sender(), targetMember);
            declareTrusted(targetMember);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            LOGGER.trace("No PingAck on PingReq within {}ms (pingers={}, target={})", randomMembers, targetMember,
                timeout);
            declareSuspected(targetMember);
          }
        }));

    FailureDetectorData pingReqData = new FailureDetectorData(localEndpoint, targetMember);
    Message message = new Message(pingReqData, TransportHeaders.QUALIFIER, PING_REQ,
                                  TransportHeaders.CORRELATION_ID, period/* correlationId */);
    for (TransportEndpoint randomMember : randomMembers) {
      LOGGER.trace("Send PingReq from {} to {}", localEndpoint, randomMember);
      send(randomMember, message);
    }
  }

  /**
   * Adds given member to {@link #suspectedMembers} and emitting state {@code SUSPECTED}.
   */
  private void declareSuspected(TransportEndpoint member) {
    if (suspectedMembers.add(member)) {
      LOGGER.debug("Member {} became SUSPECTED", member);
      subject.onNext(suspected(member));
    }
  }

  /**
   * Removes given member from {@link #suspectedMembers} and emitting state {@code TRUSTED}.
   */
  private void declareTrusted(TransportEndpoint member) {
    if (suspectedMembers.remove(member)) {
      LOGGER.debug("Member {} became TRUSTED", member);
      subject.onNext(trusted(member));
    }
  }

  private void send(TransportEndpoint endpoint, Message message) {
    transport.send(endpoint, message);
  }

  private TransportEndpoint selectPingMember(List<TransportEndpoint> members) {
    if (pingMember != null) {
      return pingMember;
    }
    return members.isEmpty() ? null : selectRandomMembers(members, 1, null).get(0);
  }

  private List<TransportEndpoint> selectRandomMembers(List<TransportEndpoint> members, int count,
      TransportEndpoint memberToExclude) {
    if (randomMembers != null) {
      return randomMembers;
    }

    checkArgument(count > 0, "FailureDetector: k is required!");
    count = min(count, 5);

    List<TransportEndpoint> list = new ArrayList<>(members);
    list.remove(memberToExclude);

    List<TransportEndpoint> target = new ArrayList<>(count);
    for (; !list.isEmpty() && count != 0; count--) {
      TransportEndpoint member = list.get(ThreadLocalRandom.current().nextInt(list.size()));
      target.add(member);
      list.remove(member);
    }
    return target;
  }

  private Func1<Message, Boolean> ackFilter(String correlationId) {
    return new TransportHeaders.Filter(ACK, correlationId);
  }

  private Func1<Message, Boolean> targetFilter(final TransportEndpoint endpoint) {
    return new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        FailureDetectorData data = message.data();
        return data.getTo().equals(endpoint);
      }
    };
  }

  private static class CorrelationFilter implements Func1<Message, Boolean> {
    final TransportEndpoint from;
    final TransportEndpoint target;

    CorrelationFilter(TransportEndpoint from, TransportEndpoint target) {
      this.from = from;
      this.target = target;
    }

    @Override
    public Boolean call(Message message) {
      FailureDetectorData data = message.data();
      return from.equals(data.getFrom()) && target.equals(data.getTo()) && data.getOriginalIssuer() == null;
    }
  }
}
