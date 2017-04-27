FROM ubuntu
MAINTAINER Logsape <support@logscape.com>
EXPOSE 8080
ARG VERSION
RUN apt-get update && apt-get install -y default-jdk wget unzip vim net-tools sysstat && rm -rf /var/lib/apt/lists/*
RUN wget http://logscape.com/download/Release_${VERSION}/Logscape-${VERSION}.zip && unzip Logscape-${VERSION}.zip && rm Logscape-${VERSION}.zip;
WORKDIR /logscape
ENTRYPOINT /logscape/logscape.sh start; tail -f ./work/agent.log;


#docker build . -t logscape/logscape:latest -t logscape/logscape:[build number] --build-args VERSION=[build number]