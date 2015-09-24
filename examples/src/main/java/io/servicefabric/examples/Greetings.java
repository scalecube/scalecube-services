package io.servicefabric.examples;

import io.protostuff.Tag;
import io.servicefabric.transport.Message;
import rx.functions.Func1;

public final class Greetings {

  public static final Func1<Message, Boolean> MSG_FILTER = new Func1<Message, Boolean>() {
    @Override
    public Boolean call(Message message) {
      return message.data() != null && Greetings.class.equals(message.data().getClass());
    }
  };

  @Tag(1)
  String quote;

  public Greetings() {}

  public Greetings(String quote) {
    this.quote = quote;
  }

  public String getQuote() {
    return quote;
  }

  public void setQuote(String quote) {
    this.quote = quote;
  }

  @Override
  public String toString() {
    return "Greetings [quote=" + quote + "]";
  }
}
