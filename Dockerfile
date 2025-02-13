# Build the Go binary.
FROM golang:latest AS go
ARG CGO_ENABLED=0
WORKDIR /app
COPY . /app
ENV GOTOOLCHAIN=auto
RUN go build

# Build container.
FROM python:3.13
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
COPY --from=go /app/alfred-xref /app/alfred-xref
COPY xref.py /app/xref.py
COPY setup.sh /app/setup.sh
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -q -r requirements.txt
RUN ./setup.sh

# Run
CMD ["/app/alfred-xref"]
