FROM golang:1.16

WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...

CMD ["go","run","main.go"]