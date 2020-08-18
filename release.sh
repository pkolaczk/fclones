#!/bin/sh
set -e

# This script generates packages for a release and places them in target/packages
rustup update
cargo fmt -- --check
cargo clippy --all
cargo test

VERSION=$(cargo pkgid | sed 's/.*#//')
PKG_DIR=target/packages
mkdir -p $PKG_DIR

cargo deb
mv target/debian/*.deb $PKG_DIR

fakeroot alien --to-rpm --to-tgz -c -v $PKG_DIR/*.deb
mv *.rpm $PKG_DIR
mv *.tgz $PKG_DIR

rustup target add x86_64-pc-windows-gnu
cargo build --release --target=x86_64-pc-windows-gnu
zip -j $PKG_DIR/"fclones-$VERSION-win.x86_64.zip" target/x86_64-pc-windows-gnu/release/fclones.exe

