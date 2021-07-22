FROM debian:latest

LABEL maintainer="tjveil@gmail.com"

ARG VERSION

RUN apt-get update \
    && apt-get install -y zip make gcc \
    && mkdir -v /opt/tpch \
    && mkdir -v /opt/tpch-output

COPY tpc-h-tool.zip /opt/tpch/tpc-h-tool.zip

RUN cd /opt/tpch \
    && unzip /opt/tpch/tpc-h-tool.zip \
    && rm -rf /opt/tpch/tpc-h-tool.zip \
    && cd ${VERSION}/dbgen

COPY makefile /opt/tpch/${VERSION}/dbgen/makefile

RUN cd /opt/tpch/${VERSION}/dbgen \
    && make

WORKDIR /opt/tpch/${VERSION}/dbgen

CMD ./dbgen -s ${SCALE} && mv -v *.tbl /opt/tpch-output
