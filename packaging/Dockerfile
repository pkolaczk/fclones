FROM rust:1.70-bookworm
RUN apt-get update
RUN apt-get install -y fakeroot alien gcc-mingw-w64-x86-64 zip libgtk-4-dev libadwaita-1-dev
RUN rustup toolchain install stable-x86_64-pc-windows-gnu
RUN rustup target add x86_64-unknown-linux-musl
RUN rustup target add i686-unknown-linux-musl
RUN rustup target add x86_64-pc-windows-gnu
RUN cargo install cargo-deb
RUN mkdir /rpmbuild
RUN chmod -R a+rwx /rpmbuild
RUN chmod -R a+rwx /usr/local/cargo/registry

