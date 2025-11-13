#!/bin/bash
set -x

image_build() {
    docker build -t rustdfs-env:latest .
}

image_run() {
    docker run --rm -it \
        -v "$(pwd)/..":/rustDFS \
        -w /rustDFS \
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
