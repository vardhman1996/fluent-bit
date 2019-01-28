FROM debian:stretch as builder

# Fluent Bit version
ENV FLB_MAJOR 1
ENV FLB_MINOR 1
ENV FLB_PATCH 0
ENV FLB_VERSION 1.1.0

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      make \
      wget \
      unzip \
      libssl1.0-dev \
      libasl-dev \
      libsasl2-dev \
      pkg-config \
      libsystemd-dev \
      zlib1g-dev \
      libcurl4-openssl-dev \
      liblog4cxx-dev \
      libprotobuf-dev \
      libboost-all-dev \
      libgtest-dev \
      google-mock \
      libjsoncpp-dev \
      libxml2-utils \ 
      protobuf-compiler \
      python-setuptools

RUN mkdir -p /fluent-bit/bin /fluent-bit/etc /fluent-bit/log /tmp/src/
COPY . /tmp/src/
RUN rm -rf /tmp/src/build/*

RUN dpkg -i /tmp/src/plugins/out_pulsar/apache-pulsar-client.deb && \
    dpkg -i /tmp/src/plugins/out_pulsar/apache-pulsar-client-dev.deb

RUN dpkg -L apache-pulsar-client && dpkg -L apache-pulsar-client-dev
    
WORKDIR /tmp/src/build/
RUN cmake -DFLB_DEBUG=Off \
          -DFLB_TRACE=Off \
          -DFLB_JEMALLOC=On \
          -DFLB_BUFFERING=On \
          -DFLB_TLS=On \
          -DFLB_SHARED_LIB=Off \
          -DFLB_EXAMPLES=Off \
          -DFLB_HTTP_SERVER=On \
          -DFLB_IN_SYSTEMD=On \
          -DFLB_OUT_KAFKA=On ..

RUN make -j $(getconf _NPROCESSORS_ONLN)
RUN install bin/fluent-bit /fluent-bit/bin/

# Configuration files
COPY conf/fluent-bit.conf \
     conf/parsers.conf \
     conf/parsers_java.conf \
     conf/parsers_extra.conf \
     conf/parsers_openstack.conf \
     conf/parsers_cinder.conf \
     conf/plugins.conf \
     /fluent-bit/etc/

FROM debian:stretch

COPY --from=builder /usr/lib/x86_64-linux-gnu/*sasl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libz* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libz* /lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssl.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto.so* /usr/lib/x86_64-linux-gnu/
# These below are all needed for systemd
COPY --from=builder /lib/x86_64-linux-gnu/libsystemd* /lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libselinux.so* /lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/liblzma.so* /lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/liblz4.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libgcrypt.so* /lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libpcre.so* /lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libgpg-error.so* /lib/x86_64-linux-gnu/
COPY --from=builder /tmp/src/plugins/out_pulsar/apache-pulsar-client.deb /tmp/
COPY --from=builder /tmp/src/plugins/out_pulsar/apache-pulsar-client-dev.deb /tmp/

COPY --from=builder /fluent-bit /fluent-bit

RUN dpkg -i /tmp/apache-pulsar-client.deb && \
    dpkg -i /tmp/apache-pulsar-client-dev.deb

#
EXPOSE 2020

# Entry point
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf"]
