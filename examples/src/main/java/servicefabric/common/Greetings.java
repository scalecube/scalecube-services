package servicefabric.common;

import io.protostuff.Tag;

public final class Greetings {

	

	@Tag(1)
	String quote;

	public Greetings(){};
	
	public Greetings(String quote){
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
