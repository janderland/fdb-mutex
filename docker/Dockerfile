# Build environment for fdb-mutex library.
FROM debian:12

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      build-essential=12.9 \
      ca-certificates=20* \
      git=1:2.39.* \
      curl=7.88.* && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install FoundationDB client library.
ARG FDB_VERSION=7.4.5
RUN curl -Lo /fdb.deb "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" && \
    dpkg -i /fdb.deb && \
    rm /fdb.deb

# Install Go.
ARG GO_VERSION=1.23.4
RUN curl -Lo /go.tar.gz "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" && \
    tar -C /usr/local -xzf /go.tar.gz && \
    rm /go.tar.gz
ENV PATH="/root/go/bin:/usr/local/go/bin:${PATH}"
ENV GOCACHE="/cache/gocache"
ENV GOMODCACHE="/cache/gomod"

# Install golangci-lint.
ARG GOLANGCI_LINT_VERSION=v1.62.2
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
    sh -s -- -b "$(go env GOPATH)/bin" ${GOLANGCI_LINT_VERSION}
ENV GOLANGCI_LINT_CACHE="/cache/golangci-lint"

# Install shellcheck.
ARG SHELLCHECK_VERSION=v0.10.0
RUN curl -Lo /shellcheck.tar.xz "https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/shellcheck-${SHELLCHECK_VERSION}.linux.x86_64.tar.xz" && \
    tar -xf /shellcheck.tar.xz && \
    mv /shellcheck-*/shellcheck /usr/local/bin && \
    rm -r /shellcheck.tar.xz /shellcheck-*

# Install hadolint.
ARG HADOLINT_VERSION=v2.12.0
RUN curl -Lo /usr/local/bin/hadolint "https://github.com/hadolint/hadolint/releases/download/${HADOLINT_VERSION}/hadolint-Linux-x86_64" && \
    chmod +x /usr/local/bin/hadolint

# Install jq for database setup script.
RUN curl -Lo /usr/local/bin/jq "https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64" && \
    chmod +x /usr/local/bin/jq

# Configure git to allow any user to run git commands on the mounted directory.
RUN git config --global --add safe.directory /src

WORKDIR /src
