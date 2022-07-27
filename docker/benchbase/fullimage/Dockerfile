# TODO: use a smaller base image that only has the jre, not the full jdk.
FROM --platform=linux maven:3.8.5-eclipse-temurin-17

# Add a containeruser that allows vscode/codespaces to map the local host user
# (often uid 1000) to some non-root user inside the container.
ARG CONTAINERUSER_UID=1000
ARG CONTAINERUSER_GID=1000
RUN groupadd --non-unique --gid ${CONTAINERUSER_GID} containergroup \
    && useradd --non-unique --create-home --no-user-group --comment 'Container User' \
        --uid ${CONTAINERUSER_UID} --gid ${CONTAINERUSER_GID} containeruser
RUN mkdir -p /benchbase/results && chown -R containeruser:containergroup /benchbase/
WORKDIR /benchbase

# Copy the full source into the container image.
# Assumes the context is given as the root of the repo.

USER containeruser

VOLUME /benchbase/results

COPY --chown=containeruser:containergroup ./ /benchbase/
# Move the built profiles into place.
RUN mkdir -p /benchbase/profiles/ && mv /benchbase/.docker-build-stage/* /benchbase/profiles/

ENV BENCHBASE_PROFILE=postgres
ENTRYPOINT ["/benchbase/docker/benchbase/container-files/entrypoint.sh"]
CMD ["--help"]
