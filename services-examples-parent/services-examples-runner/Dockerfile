FROM openjdk:11

ARG EXECUTABLE_JAR

WORKDIR /opt/scalecube

ENV DEFAULT_JAVA_OPTS="-server \
-XX:+DisableExplicitGC \
-Dsun.rmi.dgc.client.gcInterval=3600000 \
-Dsun.rmi.dgc.server.gcInterval=3600000"

ENV DEFAULT_JMX_OPTS="-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=5678 \
-Dcom.sun.management.jmxremote.rmi.port=5678 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.local.only=false \
-Djava.rmi.server.hostname=127.0.0.1"

ENV DEFAULT_OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=services-examples-oom.hprof \
-XX:+UseGCOverheadLimit"

COPY target/lib lib
COPY target/${EXECUTABLE_JAR}.jar app.jar

# profiler agent port
EXPOSE 10001

# jmx server port
EXPOSE 5678

ENTRYPOINT exec java \
$DEFAULT_JAVA_OPTS \
$JAVA_OPTS \
$DEFAULT_JMX_OPTS \
$DEFAULT_OOM_OPTS \
-Dlog4j.configurationFile=log4j2.xml \
-jar app.jar $PROGRAM_ARGS
