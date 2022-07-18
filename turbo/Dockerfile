FROM eclipse-temurin:8u332-b09-jre-alpine

RUN mkdir /etc/fk-turbo

#---------------------------------- WARNING ----------------------------
#   Run maven clean install locally before building docker image
# ----------------------------------------------------------------------
RUN mkdir -p /var/log/flipkart/supply-chain/fk-turbo/
RUN mkdir -p /etc/fk-turbo/turbo-charts/config/

COPY target/turbo-relayer-3.0.7.jar /var/lib/fk-turbo/relayer.jar
COPY server.yml /etc/fk-turbo/serverconfig/server.yml
RUN sha1sum /var/lib/fk-turbo/relayer.jar > /var/lib/fk-turbo/jarsha1sum



# ------------- For Local Docker Debugging -----------------------------
#COPY relayer.yml  /etc/fk-turbo/config/relayer.yml
#RUN sed -i "s/\localhost:/host.docker.internal:/" /etc/fk-turbo/config/relayer.yml
#COPY local-docker/mysql.* /etc/fk-turbo/config/secrets/



ENV MEMORY="1024m"
ENV THREADSTACKSIZE_RELAYER="512k"
ENV preventReRelayOnStartUp="true"
ENV forceStart="true"
ENV partitionManagementOnStartup="true"

ENV GC_OPTS="-XX:NewRatio=2 -XX:+UseConcMarkSweepGC -XX:SurvivorRatio=4 -XX:CompileThreshold=100 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:/var/log/flipkart/supply-chain/fk-turbo/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=128K" \
    JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=27755 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.rmi.port=27755 -Djava.rmi.server.hostname=127.0.0.1 " \
    JFR_OPTS="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" \
    DEBUG_OPTS="-DloadConfigFromLocal=true -DloadConfigFilePath=server.json"



ARG user=turbouser
ARG group=turbogroup
ARG uid=8888
ARG gid=8888
RUN addgroup -g ${gid} -S ${group} \
    && adduser -u ${uid} -g ${gid} -S ${user} \
    && chown -R ${user}:${group}  /var/lib/fk-turbo/ /etc/fk-turbo/ /var/log/flipkart/


USER turbouser

WORKDIR /etc/fk-turbo/

EXPOSE 36005 36006 27755

ENTRYPOINT  exec java "-Xms${MEMORY}" "-Xmx${MEMORY}" "-Xss${THREADSTACKSIZE_RELAYER}" "-XX:ReservedCodeCacheSize=128m"\
            $GC_OPTS $JMX_OPTS $JFR_OPTS $DEBUG_OPTS \
            "-Drelayer.fcpAppId=${fcpAppId}" \
            "-Drelayer.fcpZone=${fcpZone}" \
            "-Drelayer.preventReRelayOnStartUp=${preventReRelayOnStartUp}"\
            "-Drelayer.forceStart=${forceStart}"\
            "-Drelayer.partitionManagementOnStartup=${partitionManagementOnStartup}" \
            -jar /var/lib/fk-turbo/relayer.jar server "/etc/fk-turbo/serverconfig/server.yml"