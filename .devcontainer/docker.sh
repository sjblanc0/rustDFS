#!/bin/bash
set -x

image_build() {
    docker build \
        -f .devcontainer/Dockerfile \
        -t rustdfs-env:latest .
}

image_run() {
    docker run -P --rm -it \
        -v "$(pwd)":/root/rustDFS \
        -w /root/rustDFS \
        rustdfs-env:latest \
        bash
}

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 {build|run}"
    exit 1
fi

case $1 in
    build)
        image_build
        ;;
    run)
        image_run
        ;;
    *)
        echo "Invalid command. Use 'build' or 'run'."
        exit 1
        ;;
esac
