package io.scalecube.leaderelection;

public class AppendEntriesResponse {

  public static final String QUALIFIER = "sc/raft/appendentries/response";

  // leader's term
  private final int term;

  // candidate requesting vote
  private final boolean success;

  @Override
  public String toString() {
    return "AppendEntriesResponse [term=" + term + ", success=" + success + "]";
  }

  public AppendEntriesResponse(int term, boolean success) {
    this.term = term;
    this.success = success;
  }

  public int term() {
    return term;
  }

  public boolean success() {
    return success;
  }
}
