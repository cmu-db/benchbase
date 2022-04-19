# A simple dev environment container for benchbase.
#
# - Build:
#
#   - Full image:
#
#     docker build -t benchbase -f Dockerfile ../..
#
#   - Dev only:
#
#     # Skip copying and building the source into the devcontainer image since we will map it in later.
#     docker build -t benchbase-dev -f Dockerfile --target devcontainer ../..
#
# - Run:
#
#   - Full image:
#
#     # Map the config to read in and a place for the results to be written out.
#     docker run -it --rm --name benchbase -v $PWD/config:/benchbase/config -v $PWD/results:/benchbase/results --env BENCHBASE_PROFILE=postgres benchbase -- <benchbase args>
#
#   - Dev image:
#
#     # Map the whole source directory into the container.
#     # Optionally build the source as the container launch executable.
#     docker run -it --rm --name benchbase-dev -v $PWD:/benchbase benchbase-dev mvn clean package -P postgres

FROM --platform=linux maven:3.8.5-eclipse-temurin-17 AS devcontainer

# Add a containeruser that allows vscode/codespaces to map the local host user
# (often uid 1000) to some non-root user inside the container.
RUN addgroup --gid 1000 containergroup \
    && adduser --disabled-password --gecos 'Container User' --uid 1000 --gid 1000 containeruser
USER containeruser

WORKDIR /benchbase
RUN mkdir -p /benchbase/results && chown containeruser:containergroup /benchbase/results

# When running the devcontainer, just launch an interactive shell by default.
ENTRYPOINT ["/bin/bash", "-l"]

# Copy the full source into the container image.
# Assumes the context is given as the root of the repo.

# Preload some dependencies.
COPY --chown=containeruser:containergroup pom.xml /benchbase/pom.xml
COPY --chown=containeruser:containergroup .git/ /benchbase/.git/
RUN mvn initialize

# Add an additional layer that also includes a built copy of the source.
FROM devcontainer AS fullimage

ARG MAVEN_OPTS
USER containeruser

VOLUME /benchbase/results

COPY --chown=containeruser:containergroup ./ /benchbase/
# Uncomment for slightly faster incremental testing at the expense of additional docker image layers.
RUN mvn process-resources compile test-compile # test
# Build all of the profiles into the image.
ARG BENCHBASE_PROFILES="cockroachdb mariadb mysql postgres spanner phoenix"
RUN for profile in ${BENCHBASE_PROFILES}; do \
        mvn --file pom.xml package -P $profile -D skipTests || exit 1; \
        mkdir -p profiles/$profile; \
        tar -C profiles/$profile/ --strip-components=1 -xvzf target/benchbase-$profile.tgz || exit 1; \
        rm -rf profiles/$profile/data/ && ln -s ../../data profiles/$profile/data; \
    done \
    && mvn clean \
    && test -d data \
    && test "`readlink -f profiles/postgres/data`" = "/benchbase/data"

ENV BENCHBASE_PROFILE=postgres
ENTRYPOINT ["/benchbase/docker/benchbase/entrypoint.sh"]
CMD ["--help"]