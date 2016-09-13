package io.scalecube.services.examples;

import io.scalecube.transport.Message;

/**
 * Created by ronenn on 9/11/2016.
 */
public class Ad {
    String id;
    public Ad(Ad.Builder builder){
        this.id = builder.id;
    }
    /**
     * Instantiates new empty message builder.
     */
    public static Ad.Builder builder() {
        return new Ad.Builder();
    }

    public static class Builder {

        private String id;

        private Builder() {
        }

        public Ad.Builder id(String id) {
            this.id = id;
            return this;
        }
    }
}
