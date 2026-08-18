#!/bin/bash
sudo perf record -g -F 99 --delay 1 -o perf.data ./build/main
sudo chown $(whoami):$(whoami) perf.data