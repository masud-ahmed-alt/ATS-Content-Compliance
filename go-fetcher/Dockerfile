# ----------------------------
# Build stage
# ----------------------------
FROM golang:1.23-alpine AS build

WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Copy entire source (avoids failing when go.sum doesn't exist yet)
COPY . .

# If there's no go.mod, initialize one. Then resolve deps.
# You can override MODULE_PATH at build time if you want.
ARG MODULE_PATH=github.com/yourorg/go-fetcher
RUN if [ ! -f go.mod ]; then go mod init ${MODULE_PATH}; fi && \
    go mod tidy

# Build static binary
ENV CGO_ENABLED=0
RUN go build -o /go-fetcher -trimpath -ldflags="-s -w" main.go


# ----------------------------
# Final runtime image
# ----------------------------
FROM alpine:3.19

RUN apk add --no-cache ca-certificates
COPY --from=build /go-fetcher /usr/local/bin/go-fetcher

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/go-fetcher"]
