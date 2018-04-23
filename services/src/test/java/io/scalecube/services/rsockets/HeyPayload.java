package io.scalecube.services.rsockets;

public class HeyPayload {

    private String text;

    public HeyPayload() {
    }

    public HeyPayload(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "HeyPayload{" +
                "text='" + text + '\'' +
                '}';
    }
}
