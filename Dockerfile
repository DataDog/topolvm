ARG BASE_IMAGE

# Build topolvm
FROM registry.ddbuild.io/images/mirror/golang:1.22 AS build-topolvm

# Get argument
ARG TOPOLVM_VERSION
ARG TARGETARCH
ENV GOTOOLCHAIN auto

COPY . /workdir
WORKDIR /workdir

RUN touch pkg/lvmd/proto/*.go
RUN make build-topolvm TOPOLVM_VERSION=${TOPOLVM_VERSION} GOARCH=${TARGETARCH}

# TopoLVM container
FROM $BASE_IMAGE

ENV DEBIAN_FRONTEND=noninteractive
USER root
RUN clean-apt install \
        btrfs-progs \
        file \
        xfsprogs

COPY --from=build-topolvm /workdir/build/hypertopolvm /hypertopolvm

RUN ln -s hypertopolvm /lvmd \
    && ln -s hypertopolvm /topolvm-scheduler \
    && ln -s hypertopolvm /topolvm-node \
    && ln -s hypertopolvm /topolvm-controller

COPY --from=build-topolvm /workdir/LICENSE /LICENSE

USER dog
ENTRYPOINT ["/hypertopolvm"]
