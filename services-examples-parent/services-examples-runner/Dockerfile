FROM openjdk:11-jdk-slim

ARG EXECUTABLE_JAR
ARG SERVICE_NAME
ARG SERVICE_VERSION
ENV SERVICE_NAME ${SERVICE_NAME}
ENV SERVICE_VERSION ${SERVICE_VERSION}
ENV DUMPS_DIR /opt/scalecube/dumps
ENV LOGS_DIR /opt/scalecube/logs
ENV JMX_PORT 5678
ENV LOG4J_CONFIGURATION_FILE ${LOG4J_CONFIGURATION_FILE:-log4j2.xml}
ENV PRINT_FLAGS_FINAL ${PRINT_FLAGS_FINAL:-"-XX:+PrintFlagsFinal"}

EXPOSE $JMX_PORT

WORKDIR /opt/scalecube

RUN mkdir -p $DUMPS_DIR
RUN mkdir -p $LOGS_DIR

ENV DEFAULT_JAVA_OPTS="\
-Xlog:gc*,safepoint:$LOGS_DIR/gc.log \
-Xlog:os+container=info \
-XX:InitialRAMPercentage=50 \
-XX:MinRAMPercentage=50 \
-XX:MaxRAMPercentage=70 \
-XX:NativeMemoryTracking=detail \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=$DUMPS_DIR/$SERVICE_NAME-oom.hprof \
-Djava.net.preferIPv4Stack=true \
-Dsun.rmi.dgc.client.gcInterval=3600000 \
-Dsun.rmi.dgc.server.gcInterval=3600000 \
-Djava.rmi.server.hostname=127.0.0.1 \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.local.only=false \
-XX:+AlwaysActAsServerClassMachine \
-XX:+UnlockExperimentalVMOptions \
-XX:+TrustFinalNonStaticFields \
-XX:+UnlockDiagnosticVMOptions \
-XX:GuaranteedSafepointInterval=300000 \
-XX:BiasedLockingStartupDelay=0"

COPY target/lib lib
COPY target/${EXECUTABLE_JAR}.jar app.jar

ENTRYPOINT exec java \
$PRINT_FLAGS_FINAL \
$DEFAULT_JAVA_OPTS \
$JAVA_OPTS \
$DEFAULT_JMX_OPTS \
$DEFAULT_OOM_OPTS \
-Dcom.sun.management.jmxremote.port=$JMX_PORT \
-Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
-Dlog4j.configurationFile=$LOG4J_CONFIGURATION_FILE \
-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector \
-jar app.jar $PROGRAM_ARGS
