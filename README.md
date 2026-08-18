# High‑frequency Arbitrage Aggregator

> Real‑time system for collecting and analyzing arbitrage opportunities across multiple cryptocurrency exchanges.

![C++](https://img.shields.io/badge/C++-20-blue.svg?style=flat&logo=c%2B%2B)
![Boost](https://img.shields.io/badge/Boost-1.83+-green.svg?style=flat&logo=boost)
![CMake](https://img.shields.io/badge/CMake-3.20+-blue.svg?style=flat&logo=cmake)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## 🚀 What is this?

This is a **high‑performance arbitrage aggregator** built from scratch in modern C++. It connects to multiple cryptocurrency exchanges via WebSocket, collects real‑time market data (prices, order books), detects arbitrage opportunities, and prepares data for machine learning models.

**Why it's different:**
- Written in **low‑latency C++20** with coroutines
- Uses **lock‑free atomic data structures** for thread safety
- Supports **multiple exchanges** with dynamic symbol normalization
- Collects **structured data** for ML (HMM, XGBoost)
- Designed for **real‑time** operation, not just backtesting

---

## 🧠 Architecture Overview
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ WebSocket │────▶│ Atomic │────▶│ Aggregator │
│ Clients │ │ Matrix │ │ (Spread │
│ (Binance, │ │ (Prices + │ │ Detection) │
│ KuCoin) │ │ Depth) │ │ │
└─────────────┘ └─────────────┘ └─────────────┘
│
▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ TimerExpired│────▶│ CSV Logger │────▶│ ML Pipeline│
│ (Snapshots)│ │ (Raw Data) │ │ (HMM + │
│ │ │ │ │ XGBoost) │
└─────────────┘ └─────────────┘ └─────────────┘


### Core Components

- **`wssClient`** — abstract base class for exchange connections (WebSocket + SSL)
- **`AtomicMatrix<T>`** — lock‑free 2D storage for price data (flat array with atomic cells)
- **`DepthData`** — order book depth (20 levels) with atomic updates
- **`TimerExpired`** — delayed snapshots for supervised learning (before/after time window)
- **`Logger`** — asynchronous, thread‑safe logging with levels and exchange tags
- **`Agregator`** — spread detection and arbitration logic

---

## 🔥 Key Features

| Feature | Description |
|---------|-------------|
| **WebSocket Streaming** | Real‑time market data via Boost.Beast + SSL |
| **C++20 Coroutines** | Async I/O with `co_await` for clean code |
| **Lock‑Free Storage** | Atomic matrix for thread‑safe price updates |
| **Order Book Depth** | 20‑level depth for liquidity analysis |
| **Auto Reconnection** | Handles network failures gracefully |
| **Symbol Normalization** | Converts exchange‑specific symbols to unified format |
| **Data Collection for ML** | Records snapshots (before/after) for supervised learning |
| **Sanitizer Support** | Built‑in ThreadSanitizer & AddressSanitizer support |
| **Profiling Ready** | `perf` integration for performance tuning |

---

## 📁 Project Structure
.
├── CMakeLists.txt
├── include
│   └── agregator_
│       ├── agr.hpp
│       ├── logger.hpp
│       └── net.hpp
├── perf_start.sh
├── README.md
├── src
│   ├── agregator
│   │   ├── agr.cpp
│   │   └── agr_utils.hpp
│   ├── logger
│   │   └── logger.cpp
│   ├── main.cpp
│   └── network
│       ├── net.cpp
│       └── utils
│           ├── binance_client.cpp
│           ├── binance_client.hpp
│           ├── kucoin_client.cpp
│           ├── kucoin_client.hpp
│           ├── net_utils.cpp
│           └── net_utils.hpp
└── tsan_start.sh



---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| **C++20** | Coroutines, atomic operations, standard library |
| **Boost.Asio** | Asynchronous I/O, networking, timers |
| **Boost.Beast** | WebSocket & HTTP client (SSL/TLS) |
| **Boost.JSON** | Fast JSON parsing for market data |
| **OpenSSL** | Secure WebSocket connections |
| **CMake** | Cross‑platform build system with `FetchContent` |
| **GoogleTest** | Unit testing |
| **ThreadSanitizer** | Data race detection |
| **perf** | CPU profiling and optimization |

---

## 📊 Data Collection for ML

The system automatically collects structured data for machine learning:

### 1. Spread Signals (Supervised Learning)
- Timestamp, token, exchanges involved
- Bid/Ask prices (before and after a time window, e.g., 5 min)
- Order book depth (20 levels) for liquidity assessment
- Spread percentage (start/end)
- Volume on best levels

### 2. Market Context (Unsupervised Learning)
- Periodic snapshots of all tokens (configurable interval)
- Bid/Ask spreads across all exchanges
- Historical price sequences (for HMM)

These CSV logs are ready for training:
- **HMM** — market regime detection (trend, volatility, flat)
- **XGBoost / LightGBM** — profitability prediction for arbitrage signals

---

## 🚀 Build & Run

### Prerequisites
- **GCC 11+** or **Clang 14+**
- **CMake 3.20+**
- **OpenSSL 3.0+**

### Steps

```bash
git clone https://github.com/your-username/arbitrage-aggregator.git
cd arbitrage-aggregator
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
./main

Note: Boost is fetched automatically via CMake's FetchContent. Internet access is required during configuration.

# Enable ThreadSanitizer
cmake .. -DCMAKE_CXX_FLAGS="-fsanitize=thread -g -O1"

# Enable Debug Logs
cmake .. -DCMAKE_CXX_FLAGS="-DENABLE_DEBUG_LOGS"

# Run Tests
ctest

# Unit tests
ctest

# ThreadSanitizer (data race detection)
cmake .. -DCMAKE_CXX_FLAGS="-fsanitize=thread -g -O1" && make
./main

# CPU profiling with perf
perf record -g -F 99 ./main
perf report

🧠 Future Plans
□ Live Execution — REST API integration for order placement
□ Graph‑Based Arbitrage — Multi‑step cycle detection (Bellman‑Ford)
□ ONNX Integration — Real‑time ML inference in C++
□ Web Dashboard — Monitoring and backtesting interface
□ More Exchanges — Bybit, OKX, Kraken
□ Risk Management — Position sizing, stop‑loss, volatility filters

📝 License
MIT — free to use and modify.