#!/bin/bash
sudo add-apt-repository -y ppa:webupd8team/java
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get -y update
sudo apt -y install oracle-java8-installer oracle-java8-set-default

sudo apt -y install curl unzip


# Profiler
cd /tmp && sudo curl -nc -q -O https://www.yourkit.com/download/YourKit-JavaProfiler-2018.04-b81.zip && cd - && \
sudo wget -N https://www.yourkit.com/download/YourKit-JavaProfiler-2018.04-b81.zip -P /tmp/ && \
sudo unzip -q -o /tmp/YourKit-JavaProfiler-2018.04-b81.zip -d /usr/local && \
sudo rm /tmp/YourKit-JavaProfiler-2018.04-b81.zip

# Start application
# sudo nohup java -Dlog4j.configurationFile=log4j2-file.xml -agentpath:/usr/local/YourKit-JavaProfiler-2018.04/bin/linux-x86-64/libyjpagent.so -jar /tmp/gw.jar & > /dev/null 2>&1

# sudo java -cp /tmp/scenarios.jar -agentpath:/usr/local/YourKit-JavaProfiler-2018.04/bin/linux-x86-64/libyjpagent.so &
