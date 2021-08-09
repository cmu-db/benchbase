FROM openjdk:16-slim-buster
COPY . /usr/src/oltpbench
WORKDIR /usr/src/oltpbench
RUN .deploy/install.sh
ENV PATH="/usr/local/bin/apache-ant-1.9.15/bin:$PATH"
ENTRYPOINT [".deploy/main.sh"]
