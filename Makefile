default:
	go build -v -o ./bin/ ./cmd/memcload2

all:
	default

.PHONY: all default
