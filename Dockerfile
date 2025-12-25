ARG FENV_DOCKER_TAG=latest
FROM janderland/fenv:${FENV_DOCKER_TAG}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install Go.
ARG GO_VER=1.23.4
RUN curl -Lo /go.tar.gz https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz && \
    tar -xzf /go.tar.gz -C /usr/local && \
    rm /go.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOCACHE="/cache/gocache"
ENV GOMODCACHE="/cache/gomod"

# Install golangci-lint.
ARG GOLANGCI_LINT_VER=1.62.2
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
    sh -s -- -b /usr/local/bin v${GOLANGCI_LINT_VER}
ENV GOLANGCI_LINT_CACHE="/cache/golangci-lint"
