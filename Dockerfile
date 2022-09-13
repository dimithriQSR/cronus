FROM golang:1.19-alpine AS builder
WORKDIR /build
COPY . .
RUN go mod download && go build -o app

FROM alpine
COPY --from=builder /build/app /app
RUN mkdir /cert
EXPOSE 50052 50051
ENTRYPOINT [ "/app" ]