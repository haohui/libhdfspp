#!/bin/bash
set -ev

cd src/main/native/
rm -rf build
mkdir -p build
cd build
cmake ..
make
