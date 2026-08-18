#pragma once
#define BINANCE_HPP_
#ifdef BINANCE_HPP_
#include "net_utils.hpp"


    class wssBinanceClient : public wssClient{
        public:
            wssBinanceClient(net::io_context& ioc, std::string host, std::string sub, std::string port, 
                std::string target, Logger::Exchanges exchang, Net* net) :
                 wssClient(ioc, host, sub, port, target, exchang, net), id_{0}{};
                 net::awaitable<void> do_names() override;
        private:
            int64_t id_;
            net::awaitable<void>  do_subscribe() override;
            void parser(json::value&& jv) override;
    };


#endif // BINANCE_HPP_
