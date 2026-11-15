FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/runnel ./cmd/runnel

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/runnel /runnel
COPY --from=gcr.io/distroless/static-debian12:nonroot --chown=nonroot:nonroot /home/nonroot /data
WORKDIR /data
VOLUME /data
USER nonroot
EXPOSE 8700
ENTRYPOINT ["/runnel"]
CMD ["serve"]
