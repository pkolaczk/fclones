FROM rust:buster
RUN apt-get update
RUN apt-get install -y \
    fakeroot \
    alien \
    gcc-mingw-w64-x86-64 \
    gcc-x86-64-linux-gnu \
    zip
RUN rustup toolchain install stable-x86_64-pc-windows-gnu
RUN rustup target add x86_64-unknown-linux-gnu
RUN rustup target add x86_64-unknown-linux-musl
RUN rustup target add i686-unknown-linux-musl
RUN rustup target add x86_64-pc-windows-gnu
RUN cargo install cargo-deb
RUN mkdir /rpmbuild
RUN chmod -R a+rwx /rpmbuild
RUN chmod -R a+rwx /usr/local/cargo/registry
COPY cargo-config.toml .cargo/config.toml
