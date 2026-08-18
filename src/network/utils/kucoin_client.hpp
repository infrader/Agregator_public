#pragma once
#define BINANCE_HPP_
#ifdef BINANCE_HPP_
#include "net_utils.hpp"
#include<algorithm>


    class wssKucoinClient : public wssClient{
        public:
            wssKucoinClient(net::io_context& ioc, std::string host, std::string sub, std::string port, 
                std::string target, Logger::Exchanges exchang, Net* net) :
                 wssClient(ioc, host, sub, port, target, exchang, net), id_{0}{};
                 net::awaitable<void> do_names() override;
            ~wssKucoinClient() = default;
        private:
            int64_t id_;
            net::awaitable<void> do_connect() override;
            net::awaitable<void>  do_subscribe() override;
            void parser(json::value&& jv) override;
            net::awaitable<void> fetch_ws_url();
            bool token_fetched_ = false;
    };


#endif // BINANCE_HPP_
