FROM openjdk:8
MAINTAINER support@scalecube.io

ARG EXECUTABLE_JAR

# yourkit profile
RUN wget https://www.yourkit.com/download/docker/YourKit-JavaProfiler-2018.04-docker.zip -P /tmp/ && \
  unzip /tmp/YourKit-JavaProfiler-2018.04-docker.zip -d /usr/local && \
  rm /tmp/YourKit-JavaProfiler-2018.04-docker.zip

COPY target/lib /opt/scalecube/lib
COPY target/${EXECUTABLE_JAR}.jar /opt/scalecube/app.jar

# profiler agent port
EXPOSE 10001

ENTRYPOINT exec java -agentpath:/usr/local/YourKit-JavaProfiler-2018.04/bin/linux-x86-64/libyjpagent.so=port=10001,listen=all -Dlog4j.configurationFile=log4j2-file.xml -jar /opt/scalecube/app.jar
