#include"kucoin_client.hpp"
#include "net.hpp"
//

net::awaitable<void>  wssKucoinClient::do_subscribe(){
    json::object req;
    req["type"] = "subscribe";
    req["topic"] = "/market/ticker:all";
    req["id"] = this->id_++;
    req["response"] = true;
    std::string msg = json::serialize(req);
    co_await this->wss_.async_write(net::buffer(msg), net::use_awaitable);
    msg.clear();
    //Topic:/spotMarket/level2Depth50:{symbol},{symbol}
    auto timer = net::steady_timer(ioc_);  
    for(size_t i = 0; i < pairs.size(); ){
        req.clear();
        req["type"] = "subscribe";
        std::string topic = "/spotMarket/level2Depth50:";
        for(size_t j = 0; j < 100 && i < pairs.size(); ++j, ++i){
            topic += pairs[i] + ',';
        }
        topic.pop_back();
        req["id"] = this->id_++;
        req["response"] = true;
        req["topic"] = topic;
        msg = json::serialize(req);
        co_await this->wss_.async_write(net::buffer(msg), net::use_awaitable);
        size_t sz = co_await this->wss_.async_read(this->buff_, net::use_awaitable);
        this->buff_.consume(sz);
        timer.expires_after(std::chrono::milliseconds(200));
        co_await timer.async_wait(net::use_awaitable);
        msg.clear();
    }
};


//
net::awaitable<void> wssKucoinClient::do_names(){
    try{
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_map running", exchang);
        if(ioc_.stopped()){
            std::cerr << "io_context STOPPED" << std::endl;
        }
    #endif // ENABLE_DEBUG_LOGS
    net::ip::tcp::resolver resolver_(ioc_);
    auto result = co_await resolver_.async_resolve("api.kucoin.com", "443", net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Resolver resolve success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    ssl::context ctx(ssl::context_base::tlsv12_client);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL context params set", exchang);
    #endif // ENABLE_DEBUG_LOGS
    ssl::stream<beast::tcp_stream> stream(ioc_, ctx);
    co_await stream.next_layer().async_connect(result, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL stream connect success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    if(!SSL_set_tlsext_host_name(stream.native_handle(), "api.kucoin.com")){
        Log_Critical("SNI failure", exchang);
        throw std::runtime_error("SNI failure");
    }
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SNI success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    co_await stream.async_handshake(ssl::stream_base::client, net::use_awaitable);
     #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL stream handshacke success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    http::request<http::string_body> req;
    req.method(http::verb::get);
    req.target("/api/v1/market/allTickers");
    req.version(12);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::host, "api.kucoin.com");
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Create request body", exchang);
    #endif // ENABLE_DEBUG_LOGS
    size_t byte = co_await http::async_write(stream, req, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        std::string byte_str = std::to_string(byte);
        Log_Debug("Request success byte send: " + byte_str, exchang);
    #endif // ENABLE_DEBUG_LOGS
    beast::flat_buffer buff;
    http::response_parser<http::string_body> parser;
    parser.body_limit(20 * 1024 * 1024);
    byte = co_await http::async_read(stream, buff, parser, net::use_awaitable);
    http::response<http::string_body> res = parser.release();
    #ifdef ENABLE_DEBUG_LOGS
        byte_str = std::to_string(byte);
        Log_Debug("Response success byte get: " + byte_str, exchang);
    #endif // ENABLE_DEBUG_LOGS
    auto t = res.body();
    json::value vl = json::parse(t);
    if(vl.is_object()){
        json::object vo = vl.as_object();
        if(vo.contains("code") && vo["code"].as_string() == "200000"){
        if(vo.contains("data") && vo["data"].is_object()){
            if(vo["data"].as_object().contains("ticker") && vo["data"].as_object()["ticker"].is_array()){
            json::array symbols = vo["data"].as_object()["ticker"].as_array();
            #ifdef ENABLE_DEBUG_LOGS
                Log_Debug("Parsing...", exchang);
            #endif // ENABLE_DEBUG_LOGS
            for(auto& item : symbols){
                if(item.is_object()){
                    auto& obj = item.as_object();
                    if(obj.contains("symbol")){
                        //if(obj["symbol"].as_string().find("USDT") != std::string::npos){
                            pairs.push_back(obj["symbol"].as_string().c_str());
                    //}
                    }
                }
            }
            #ifdef ENABLE_DEBUG_LOGS
                Log_Debug("Parsing success", exchang);
            #endif // ENABLE_DEBUG_LOGS
        }
        }
    }
    }
    }catch(const std::exception& exc){
        Log_Critical(exc.what(), exchang);
    };
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_map ending", exchang);
    #endif // ENABLE_DEBUG_LOGS
};

void wssKucoinClient::parser(json::value&& jv){
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu parser start", exchang);
    #endif // ENABLE_DEBUG_LOGS
    try{
    if (jv.is_object() && jv.as_object().contains("type") && jv.as_object()["type"] == "ack") {
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Subscription confirmed", exchang);
    #endif // ENABLE_DEBUG_LOGS
        return;
    }
    if(jv.is_object()){
        auto object = jv.as_object();
            if(object.contains("topic") && object["topic"].as_string() == "/market/ticker:all"){
                    auto it = map_.find(object["subject"].as_string().c_str());
                    if(it != map_.end()){
                    if(object.contains("data") && object["data"].is_object()){
                        auto& data_obj = object["data"].as_object();
                        wssClient::Token t;
                        auto get_double = [&](std::string_view key){
                            if(!data_obj.contains(key)) return 0.0;
                            auto& val = data_obj[key];
                            if(val.is_double()) return val.as_double();
                            if(val.is_string()){
                                try{
                                    return std::stod(val.as_string().c_str());
                                }catch(...){
                                    return 0.0;
                                }
                            }return 0.0;
                        };
                        t.a = get_double("bestAsk");
                        t.b = get_double("bestBid");
                        t.qty_a = get_double("bestAskSize");
                        t.qty_b = get_double("bestBidSize");
                        net_->push_data(std::move(t), exchang, it->second);
                        }
                    }
            }else if(object.contains("subject") && object["subject"].as_string() == "level2"){
                //std::string el = object["topic"].as_string();
                size_t in = object["topic"].as_string().find_last_of(":");
                if(in != std::string::npos){
                    std::string str = object["topic"].as_string().c_str();
                    str = str.substr(in + 1);
                   auto it = map_.find(str);
                   if(it != map_.end()){
                    size_t index = it->second;
                        if(object["data"].as_object().contains("bids") && object["data"].as_object().contains("asks")){
                            if(object["data"].as_object()["bids"].is_array() && object["data"].as_object()["asks"].is_array()){
                                json::array bids = object["data"].as_object()["bids"].as_array();
                                json::array asks = object["data"].as_object()["asks"].as_array();
                                size_t level = 0;
                                for(size_t i = 0; i < std::min({static_cast<size_t>(20), asks.size(), bids.size()}); ++i){
                                    if(!bids[i].is_array() && !asks[i].is_array()) continue;
                                    auto& data_bids = bids[i].as_array();
                                    auto& data_asks = asks[i].as_array();
                                    DepthData::Level lvl_bids{std::stod(data_bids[0].as_string().c_str()), 
                                        std::stod(data_bids[1].as_string().c_str())};
                                    DepthData::Level lvl_asks{std::stod(data_asks[0].as_string().c_str()), 
                                        std::stod(data_asks[1].as_string().c_str())};
                                    net_->push_deep({lvl_bids, lvl_asks}, level, static_cast<size_t>(exchang), index);
                                    ++level;
                                }
                            }
                        }
                   }
                };
            }
        }
    }catch(const std::exception& exc){
        std::string msg = "Error parser: ";
        Log_Critical(msg + exc.what(), exchang);
    } 
};

net::awaitable<void> wssKucoinClient::do_connect(){ // dynamic endpoints
    if (!token_fetched_) {
        co_await fetch_ws_url();
        token_fetched_ = true;
    }
    co_await wssClient::do_connect();
}


net::awaitable<void> wssKucoinClient::fetch_ws_url(){
   try{
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_map running", exchang);
    #endif // ENABLE_DEBUG_LOGS
    net::ip::tcp::resolver resolver_(ioc_);
    auto result = co_await resolver_.async_resolve("api.kucoin.com", "443", net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Resolver resolve success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    ssl::context ctx(ssl::context_base::tlsv12_client);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL context params set", exchang);
    #endif // ENABLE_DEBUG_LOGS
    ssl::stream<beast::tcp_stream> stream(ioc_, ctx);
    co_await stream.next_layer().async_connect(result, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL stream connect success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    if(!SSL_set_tlsext_host_name(stream.native_handle(), "api.kucoin.com")){
        Log_Critical("SNI failure", exchang);
        throw std::runtime_error("SNI failure");
    }
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SNI success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    co_await stream.async_handshake(ssl::stream_base::client, net::use_awaitable);
     #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL stream handshacke success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    http::request<http::string_body> req;
    req.method(http::verb::post);
    req.target("/api/v1/bullet-public");
    req.version(12);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::host, "api.kucoin.com");
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Create request body", exchang);
    #endif // ENABLE_DEBUG_LOGS
    size_t byte = co_await http::async_write(stream, req, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        std::string byte_str = std::to_string(byte);
        Log_Debug("Request success byte send: " + byte_str, exchang);
    #endif // ENABLE_DEBUG_LOGS
    beast::flat_buffer buff;
    http::response<http::string_body> res;
    byte = co_await http::async_read(stream, buff, res, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        byte_str = std::to_string(byte);
        Log_Debug("Response success byte get: " + byte_str, exchang);
    #endif // ENABLE_DEBUG_LOGS
    auto t = res.body();
    json::value vl = json::parse(t);
    if(vl.is_object()){
        auto& object = vl.as_object();
        if(object.contains("code") && object["code"].as_string() == "200000" && object.contains("data")){
            target_ = "/?token=";
            target_ += object["data"].as_object()["token"].as_string().c_str();
            auto& server = object["data"].as_object()["instanceServers"].as_array();
            const std::string prefix = "wss://";
            std::string endpoint = server[0].as_object()["endpoint"].as_string().c_str();
            if (endpoint.rfind(prefix, 0) == 0) {
                host_ = endpoint.substr(prefix.size());
            } else {
                host_ = endpoint; 
            }
            if (!host_.empty() && host_.back() == '/') {
                host_.pop_back();
            }
        }
    }
}catch(const std::exception& exc){

}
};