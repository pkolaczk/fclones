FROM rust:latest
RUN apt-get update
RUN apt-get install -y fakeroot alien gcc-mingw-w64-x86-64 zip
RUN rustup component add rustfmt
RUN rustup component add clippy
RUN rustup toolchain install stable-x86_64-pc-windows-gnu
RUN rustup target add x86_64-pc-windows-gnu
RUN cargo install cargo-deb
