FROM sunbird/flink:1.13.5-scala_2.12-java11

USER root

# Use the Debian snapshot repositories matching the original
# Flink image's 2021-12-20 package environment.
RUN echo "deb http://snapshot.debian.org/archive/debian/20211220T000000Z bullseye main" > /etc/apt/sources.list && \
    echo "deb http://snapshot.debian.org/archive/debian-security/20211220T000000Z bullseye-security main" >> /etc/apt/sources.list && \
    echo "deb http://snapshot.debian.org/archive/debian/20211220T000000Z bullseye-updates main" >> /etc/apt/sources.list && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends imagemagick && \
    rm -rf /var/lib/apt/lists/*

COPY target/jobs-distribution-1.0.tar.gz /tmp

USER flink

RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/

USER root

RUN rm -f /tmp/jobs-distribution-1.0.tar.gz

USER flink
