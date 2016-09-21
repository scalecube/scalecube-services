package io.scalecube.leaderelection;

/**
 * Created by ronenn on 9/13/2016.
 */
public class RaftProtocol {
  public static final String RAFT_GOSSIP_HEARTBEAT = "raft.protocol.heartbeat";
  public static final String RAFT_GOSSIP_VOTE_REQUEST = "raft.protocol.vote.request";
  public static final String RAFT_MESSAGE_VOTE_RESPONSE = "raft.protocol.vote.response";
}
