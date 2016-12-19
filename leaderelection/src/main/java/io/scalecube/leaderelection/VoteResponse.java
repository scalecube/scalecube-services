package io.scalecube.leaderelection;

public class VoteResponse {

  public static final String QUALIFIER = "sc/raft/vote/response";

  // candidate’s term
  private final int currentTerm;

  // candidate requesting vote
  private final boolean granted;

  // the identity of the elector that cast this vote
  private final String electorId;

  public VoteResponse(int currentTerm, boolean granted, String electorId) {
    this.currentTerm = currentTerm;
    this.granted = granted;
    this.electorId = electorId;
  }

  public int currentTerm() {
    return currentTerm;
  }

  public boolean granted() {
    return granted;
  }

  public String electorId() {
    return electorId;
  }
  
  @Override
  public String toString() {
    return "VoteResponse [currentTerm=" + currentTerm + ", granted=" + granted + ", electorId=" + electorId + "]";
  }

  

}
