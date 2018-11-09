FROM openjdk:8

ARG EXECUTABLE_JAR
ENV YOURKIT_AGENT "-agentpath:/usr/local/YourKit-JavaProfiler-2018.04/bin/linux-x86-64/libyjpagent.so=port=10001,listen=all"

WORKDIR /opt/scalecube

ENV DEFAULT_JAVA_OPTS="-server \
-XX:+DisableExplicitGC \
-Dsun.rmi.dgc.client.gcInterval=3600000 \
-Dsun.rmi.dgc.server.gcInterval=3600000"

ENV DEFAULT_JMX_OPTS="-Djava.rmi.server.hostname=0.0.0.0 \
-Dcom.sun.management.jmxremote.port=5678 \
-Dcom.sun.management.jmxremote.rmi.port=5678 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false"

ENV DEFAULT_OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=dumps/oom_pid<pid>_`date`.hprof \
-XX:+UseGCOverheadLimit"

# yourkit profile
RUN wget https://www.yourkit.com/download/docker/YourKit-JavaProfiler-2018.04-docker.zip -P /tmp/ && \
  unzip /tmp/YourKit-JavaProfiler-2018.04-docker.zip -d /usr/local && \
  rm /tmp/YourKit-JavaProfiler-2018.04-docker.zip

COPY target/lib lib
COPY target/${EXECUTABLE_JAR}.jar benchmarks.jar

# profiler agent port
EXPOSE 10001

# jmx server port
EXPOSE 5678

CMD exec java $DEFAULT_JAVA_OPTS $JAVA_OPTS $DEFAULT_JMX_OPTS $DEFAULT_OOM_OPTS $YOURKIT_AGENT -Dlog4j.configurationFile=log4j2-benchmarks.xml -cp benchmarks.jar $PROGRAM_ARGS
