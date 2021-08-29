#!/bin/bash
set -e
bold=$(tput bold)
normal=$(tput sgr0)

# This script generates packages for a release and places them in target/packages/<version>.
# Don't use it directly, use package-docker.sh instead.

cd "$(dirname $0)"

echo "${bold}Running checks${normal}"
set -x
cargo fmt -- --check
cargo clippy --all
cargo test -q
set +x

echo "${bold}Building${normal}"
set -x
cargo build --release
set +x

echo "${bold}Packaging${normal}"
set -x
VERSION=$(cargo pkgid | sed 's/.*#//')
PKG_DIR=target/packages/$VERSION
mkdir -p $PKG_DIR
rm -f $PKG_DIR/*

cargo deb
mv target/debian/*.deb $PKG_DIR

fakeroot alien --to-rpm -c $PKG_DIR/*.deb
mv *.rpm $PKG_DIR
fakeroot alien --to-tgz -c $PKG_DIR/*.deb
mv *.tgz $PKG_DIR

cargo build --release --target=x86_64-pc-windows-gnu
zip -j $PKG_DIR/"fclones-$VERSION-win.x86_64.zip" target/x86_64-pc-windows-gnu/release/fclones.exe
