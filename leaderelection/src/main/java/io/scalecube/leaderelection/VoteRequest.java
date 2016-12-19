package io.scalecube.leaderelection;

public class VoteRequest {

  public static final String QUALIFIER = "sc/raft/vote/request";
  
  @Override
  public String toString() {
    return "VoteRequest [term=" + term + ", candidateId=" + candidateId + "]";
  }

  //candidate’s term
  private final int term;
  
  //candidate requesting vote
  private final String candidateId;
  
  public VoteRequest(int term, String candidateId) {
    this.term = term;
    this.candidateId = candidateId;
  }
  
  public int term() {
    return term;
  }
  
  public String candidateId() {
    return candidateId;
  }

}
