#pragma once
#define _NET_HPP_
#ifdef _NET_HPP_
#include"net_utils.hpp"
#include"binance_client.hpp"
#include"kucoin_client.hpp"
#include<boost/asio.hpp>
#include<boost/beast/core.hpp>
#include<boost/beast/ssl.hpp>
#include<boost/beast/websocket.hpp>
#include<boost/beast/version.hpp>
#include<boost/beast.hpp>
#include<coroutine>
#include<string>
#include<vector>
#include<unordered_map>
#include<memory>
#include<logger.hpp>


namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

class Net {
    public: 
        Net();
        void Start();
        void push_data(wssClient::Token&& token, Logger::Exchanges exch, size_t index);
        void push_deep(std::pair<DepthData::Level, DepthData::Level> data_de, size_t level, size_t col, size_t row);
        std::unordered_map<std::string, size_t>& get_glmap(){return global_map;}; 
        AtomicMatrix<wssClient::Token>& get_data(){return data;}; 
        DepthData& get_dpdata(){return deep_data;};
        std::vector<std::string>& get_tkmap(){return token_map;};
    private:
        std::vector<net::executor_work_guard<net::system_executor>> guard_;
        net::io_context ioc;
        std::vector<std::unique_ptr<net::io_context>> clients_ioc_;
        std::vector<std::jthread> threads_;
        std::string convert(std::string str);
        std::vector<std::unique_ptr<wssClient>> clients;
        std::unordered_map<std::string, size_t> global_map;
        std::vector<std::string> token_map;
        AtomicMatrix<wssClient::Token> data;
        DepthData deep_data;
        net::awaitable<void> build_maping();
        void set_maping_local();
};




#endif // _NET_HPP