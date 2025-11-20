# ----------------------------
# Build stage
# ----------------------------
FROM golang:1.24-alpine AS build

WORKDIR /app

RUN apk add --no-cache git ca-certificates

# Copy everything from the build context (which *is already* go-fetcher/)
COPY . .

# Download dependencies
RUN go mod tidy

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
