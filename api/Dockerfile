FROM golang:1.22-alpine AS builder
WORKDIR /app

# Install git (needed for go get)
RUN apk add --no-cache git

# Copy go.mod first, then download deps (go.sum generated automatically)
COPY go.mod .
RUN go mod download && go mod tidy || true

# Copy source and build
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o lakehouse-api .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/lakehouse-api .
EXPOSE 8080
CMD ["./lakehouse-api"]
