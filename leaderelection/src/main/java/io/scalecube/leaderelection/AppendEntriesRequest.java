package io.scalecube.leaderelection;

public class AppendEntriesRequest {

  public static final String QUALIFIER = "sc/raft/appendentries/request";

  // leader's term
  private final int term;

  // candidate requesting vote
  private final String leaderId;

  public AppendEntriesRequest(int term, String leaderId) {
    this.term = term;
    this.leaderId = leaderId;
  }

  public int term() {
    return term;
  }

  public String leaderId() {
    return leaderId;
  }

  @Override
  public String toString() {
    return "AppendEntriesRequest [term=" + term + ", leaderId=" + leaderId + "]";
  }
}
