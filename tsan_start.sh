#!/bin/sh
cd /home/infrader-linux/code/Agregator_ || exit
rm -rf build
mkdir build
cd build
cmake -DENABLE_TSAN=ON ..
make -j$(nproc)
./main