FROM openjdk:8
MAINTAINER support@scalecube.io

ARG SERVICE_NAME
ARG EXECUTABLE_JAR
ENV SERVICE_NAME ${SERVICE_NAME}

COPY target/lib /opt/scalecube/lib
COPY target/${EXECUTABLE_JAR}.jar /opt/scalecube/app.jar
ENTRYPOINT ["/usr/bin/java", "-jar", "-Dlog4j.configurationFile=log4j2-file.xml", "/opt/scalecube/app.jar"]
