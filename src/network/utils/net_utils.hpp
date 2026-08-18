#pragma once
#define NET_UTILS_HPP
#ifdef NET_UTILS_HPP
#include<boost/asio.hpp>
#include<boost/beast/core.hpp>
#include<boost/beast/ssl.hpp>
#include<boost/beast/websocket.hpp>
#include<boost/beast/version.hpp>
#include<boost/beast.hpp>
#include<boost/json.hpp>
#include<coroutine>
#include<string>
#include<vector>
#include<unordered_map>
#include<memory>
#include<exception>
#include<atomic>
#include<optional>
#include<logger.hpp>
#include<openssl/ssl.h>
#include<boost/algorithm/algorithm.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include<functional>

class Net;

namespace json = boost::json;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace ssl = net::ssl;
namespace http = beast::http;
using tcp = net::ip::tcp;

template<class T>
class AtomicMatrix{
    public: 
    struct Row{
    Row(std::atomic<T>* row, size_t cols) : row_(row), cols_(cols){};
    inline std::atomic<T>& operator[](size_t col){
            return row_[col];
        }
        private:
        size_t cols_;
        std::atomic<T>* row_;
    };
    inline Row operator[](size_t row){
        return Row(data_.get() + row * cols_, cols_);
    }
    inline void resize(size_t rows, size_t cols){
        cols_ = cols;
        rows_ = rows;
        data_.reset(new std::atomic<T>[rows * cols]);
        T zero{0.0,0.0,0.0,0.0};
        for(size_t i = 0; i < rows * cols; ++i){
            data_[i].store(zero, std::memory_order_release);
        }
    }
    AtomicMatrix(): rows_(0), cols_(0), data_(nullptr){};
    size_t cols_size(){return cols_;};
    size_t rows_size(){return rows_;};
    private:
        size_t cols_ = 0;
        size_t rows_ = 0;
        std::unique_ptr<std::atomic<T>[]> data_;
};

struct alignas(64) DepthData{
    public:
    struct Level{
        double price = 0;
        double vol = 0;
    };
        void resize(size_t levels, size_t cols, size_t rows){
            levels_ = levels; cols_ = cols; rows_ = rows;
            size_t total = levels * cols * rows; size_ = total;
            const size_t alignment = 64;
            auto allocate = [total, alignment](std::unique_ptr<std::atomic<Level>[], Deleter>& ptr){
                void* raw = std::aligned_alloc(alignment, total * sizeof(std::atomic<Level>));
                if(!raw) throw std::bad_alloc();
                auto* arr = new(raw) std::atomic<Level>[total];
                for(size_t i = 0; i < total; ++i){
                    arr[i].store(Level{0.0, 0.0}, std::memory_order_release);
                }
                ptr.reset(arr);
            };
            allocate(asks);
            allocate(bids);
        };
        void bids_emplace(size_t level, size_t col, size_t row, Level data){
            try{
            size_t index = get_index(level, col, row);
            bids[index].store(data, std::memory_order_release);
            }catch(const std::exception& exc){
                Log_Warn(exc.what(), Logger::Exchanges::UNKNOW);
                throw exc;
            }
        };
        void asks_emplace(size_t level, size_t col, size_t row, Level data){
            try{
            size_t index = get_index(level, col, row);
            asks[index].store(data, std::memory_order_release);
            }catch(const std::exception& exc){
                Log_Warn(exc.what(), Logger::Exchanges::UNKNOW);
                throw exc;
            }
        };
        Level bids_at (size_t level, size_t col, size_t row) const{
            try{
            size_t index = get_index(level, col, row);
            return bids[index].load(std::memory_order_acquire);
            }catch(const std::exception& exc){
                Log_Warn(exc.what(), Logger::Exchanges::UNKNOW);
                throw exc;
            }
        };
        Level asks_at (size_t level, size_t col, size_t row) const{
            try{
            size_t index = get_index(level, col, row);
            return asks[index].load(std::memory_order_acquire);
            }catch(const std::exception& exc){
                Log_Warn(exc.what(), Logger::Exchanges::UNKNOW);
                throw exc;
            }
        };
    class Deleter{
        public:
        void operator()(std::atomic<Level>* p){
            std::free(p);
        }
    };
    private:
    size_t get_index (size_t level, size_t col, size_t row) const{
        size_t index = (col * levels_ + level) * rows_ + row;
        if(index >= size_ || level >= levels_ || col >= cols_ || row >= rows_) throw std::runtime_error("Invalid index");
        return index;
    }
    size_t levels_ = 0;
    size_t cols_ = 0; // exchanges
    size_t rows_ = 0; // tokens
    size_t size_ = 0;
    std::unique_ptr<std::atomic<Level>[], Deleter> asks{nullptr};
    std::unique_ptr<std::atomic<Level>[], Deleter> bids{nullptr};
};

class wssClient : std::enable_shared_from_this<wssClient>{
    public:
        wssClient(net::io_context& ioc, std::string host, std::string sub,
             std::string port, std::string target, Logger::Exchanges exchang, Net* net) :
         ioc_(ioc), host_(host), sub_(sub), port_(port),ctx_(ssl::context_base::tlsv12_client),exchang(exchang),
          ssl_stream_(ioc_, ctx_), wss_(ssl_stream_), target_(target), request_id_(0), buff_upload(false), net_(net){};
        net::awaitable<void> virtual do_names() = 0;
        std::vector<std::string>& get_pairs();
        net::awaitable<void> run_forever();
        void set_global_size_(size_t&& size);
        void set_map(std::unordered_map<std::string, size_t>&& map);
        struct alignas(64) Token{
            double b;
            double qty_b;
            double a;
            double qty_a;
        };
    protected:
        size_t global_size_;
        std::atomic<bool> reconnect;
        Logger::Exchanges exchang;
        std::vector<std::string> pairs;
        std::unordered_map<std::string, size_t> map_;
        net::awaitable<void> virtual do_connect();
        net::awaitable<void> virtual do_subscribe() = 0;
        net::awaitable<void> do_read();
        void virtual parser(json::value&& jv) = 0;
        std::string host_;
        beast::flat_buffer buff_;
        std::atomic<bool> buff_upload;
        json::object req_;
        std::string port_;
        std::string sub_;
        std::string target_;
        int request_id_;
        Net* net_;
        net::io_context& ioc_;
        ssl::context ctx_;
        ssl::stream<beast::tcp_stream> ssl_stream_;
        websocket::stream<ssl::stream<beast::tcp_stream>&> wss_;
};

#endif // NET_UTILS_HPP