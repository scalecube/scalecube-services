GC_OPTS="-XX:+UseParallelGC -XX:+ParallelOldGC -XX:+UseG1GC -Xms3g -Xmx3g"
PROFILER_OPTS="-agentpath:/opt/gatling/bin/linux-x86-64/libyjpagent.so"
ON_OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/`date "+%Y/%m/%d-%H:%M:%S"`.hprof"

export JAVA_OPTS="-server $GC_OPTS $PROFILER_OPTS $ON_OOM_OPTS ${JAVA_OPTS}"