FROM openjdk:8
MAINTAINER support@scalecube.io

ARG SERVICE_NAME
ARG EXECUTABLE_JAR
ENV SERVICE_NAME ${SERVICE_NAME}

# yourkit profile
RUN wget https://www.yourkit.com/download/docker/YourKit-JavaProfiler-2018.04-docker.zip -P /tmp/ && \
  unzip /tmp/YourKit-JavaProfiler-2018.04-docker.zip -d /usr/local && \
  rm /tmp/YourKit-JavaProfiler-2018.04-docker.zip

COPY target/lib /opt/scalecube/lib
COPY target/${EXECUTABLE_JAR}.jar /opt/scalecube/app.jar

# profiler agent port
EXPOSE 10001

ENTRYPOINT ["/usr/bin/java", "-jar", "-Dlog4j.configurationFile=log4j2-file.xml", "/opt/scalecube/app.jar"]
