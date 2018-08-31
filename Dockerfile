FROM openjdk:8
MAINTAINER support@scalecube.io

ARG SERVICE_NAME
ARG EXECUTABLE_JAR
ENV SERVICE_NAME $SERVICE_NAME
ENV YOURKIT_AGENT "-agentpath:/usr/local/YourKit-JavaProfiler-2018.04/bin/linux-x86-64/libyjpagent.so=port=10001,listen=all"

WORKDIR /opt/scalecube

# yourkit profile
RUN wget https://www.yourkit.com/download/docker/YourKit-JavaProfiler-2018.04-docker.zip -P /tmp/ && \
  unzip /tmp/YourKit-JavaProfiler-2018.04-docker.zip -d /usr/local && \
  rm /tmp/YourKit-JavaProfiler-2018.04-docker.zip

COPY target/lib lib
COPY target/${EXECUTABLE_JAR}.jar benchmarks.jar

# profiler agent port
EXPOSE 10001

CMD exec java $JAVA_OPTS $YOURKIT_AGENT -Dlog4j.configurationFile=log4j2-benchmarks.xml -cp benchmarks.jar $PROGRAM_ARGS
