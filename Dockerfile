FROM golang:1.20 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN make build.linux

FROM registry.opensource.zalan.do/library/alpine-3.13:latest

WORKDIR /app

COPY --from=builder /app/build/linux/es-operator ./

LABEL maintainer="Team Lens @ Zalando SE <team-lens@zalando.de>"

ENTRYPOINT ["/app/es-operator"]