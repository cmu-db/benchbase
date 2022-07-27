FROM --platform=linux maven:3.8.5-eclipse-temurin-17 AS devcontainer

# Add a containeruser that allows vscode/codespaces to map the local host user
# (often uid 1000) to some non-root user inside the container.
ARG CONTAINERUSER_UID=1000
ARG CONTAINERUSER_GID=1000
RUN groupadd --non-unique --gid ${CONTAINERUSER_GID} containergroup \
    && useradd --non-unique --create-home --no-user-group --comment 'Container User' \
        --uid ${CONTAINERUSER_UID} --gid ${CONTAINERUSER_GID} containeruser
RUN mkdir -p /benchbase/results && chown -R containeruser:containergroup /benchbase/
USER containeruser
ENV MAVEN_CONFIG=/home/containeruser/.m2
WORKDIR /benchbase

# When running the devcontainer, just launch an interactive shell by default.
ENTRYPOINT ["/bin/bash", "-l"]

# Copy the full source into the container image.
# Assumes the context is given as the root of the repo.

# Preload some dependencies.
COPY --chown=containeruser:containergroup pom.xml /benchbase/pom.xml
COPY --chown=containeruser:containergroup .git/ /benchbase/.git/
ARG MAVEN_OPTS
RUN mvn -T2C -B --file pom.xml initialize \
    && rm -rf /benchbase/.git /benchbase/target /benchbase/pom.xml
