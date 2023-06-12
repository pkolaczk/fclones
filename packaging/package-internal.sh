#!/bin/bash
set -e
bold=$(tput bold)
normal=$(tput sgr0)

# This script generates packages for a release and places them in target/packages/<version>.
# Don't use it directly, use package.sh instead.

cd "$(dirname $0)/.."

echo "${bold}Building${normal}"
set -x
cargo build -p fclones -p fclones-gui --release
cargo build --release --target i686-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
cargo build --release --target x86_64-pc-windows-gnu
set +x

echo "${bold}Packaging${normal} fclones"
set -x
VERSION=$(cargo pkgid -p fclones | sed 's/.*#//')
PKG_DIR=target/packages/fclones-$VERSION
mkdir -p $PKG_DIR
rm -f $PKG_DIR/*

cargo deb -p fclones
mv target/debian/*.deb $PKG_DIR

fakeroot alien --to-rpm -c $PKG_DIR/*.deb
mv *.rpm $PKG_DIR
fakeroot alien --to-tgz -c $PKG_DIR/*.deb
mv *.tgz $PKG_DIR/"fclones-$VERSION-linux-glibc-x86_64.tar.gz"

tar -zcvf $PKG_DIR/"fclones-$VERSION-linux-musl-x86_64.tar.gz" target/x86_64-unknown-linux-musl/release/fclones
tar -zcvf $PKG_DIR/"fclones-$VERSION-linux-musl-i686.tar.gz" target/i686-unknown-linux-musl/release/fclones
zip -j $PKG_DIR/"fclones-$VERSION-windows-x86_64.zip" target/x86_64-pc-windows-gnu/release/fclones.exe

echo "${bold}Packaging${normal} fclones-gui"
set -x
VERSION=$(cargo pkgid -p fclones-gui | sed 's/.*#//')
PKG_DIR=target/packages/fclones-gui-$VERSION
mkdir -p $PKG_DIR
rm -f $PKG_DIR/*

cargo deb -p fclones-gui
mv target/debian/*.deb $PKG_DIR

fakeroot alien --to-rpm -c $PKG_DIR/*.deb
mv *.rpm $PKG_DIR
fakeroot alien --to-tgz -c $PKG_DIR/*.deb
mv *.tgz $PKG_DIR/"fclones-gui-$VERSION-linux-glibc-x86_64.tar.gz"
