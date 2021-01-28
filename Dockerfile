FROM gradle:4.7.0-jdk8-alpine AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean build buildRelease -x test --no-daemon

FROM openjdk:8-jre-slim
RUN apt-get update && apt-get --yes upgrade && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /home/gradle/src/build/output/kafka-gitops /usr/local/bin/kafka-gitops