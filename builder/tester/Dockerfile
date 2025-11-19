FROM golang:1.25 AS builder

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags='-s -w' -o /out/tester ./...

FROM scratch
COPY --from=builder /out/tester /tester

ENTRYPOINT ["/tester"]
