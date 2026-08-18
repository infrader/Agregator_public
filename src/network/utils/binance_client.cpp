#include"binance_client.hpp"
#include "net.hpp"


net::awaitable<void>  wssBinanceClient::do_subscribe(){
    const size_t BATCH_SIZE = 30;
    json::array params;
    for(const auto& it : pairs){
        std::string stream_name = boost::algorithm::to_lower_copy(it) + "@bookTicker";
        params.emplace_back(stream_name);
    }
    for(const auto& it : pairs){
        std::string stream_name = boost::algorithm::to_lower_copy(it) + "@depth20@100ms";
        params.emplace_back(stream_name);
    }
    if(params.empty()){
        Log_Critical("Fu do_sub params empty!", exchang);
        co_return;
    }
    #ifdef ENABLE_DEBUG_LOGS
        std::string params_sz = std::to_string(params.size());
        Log_Debug("Params size = " + params_sz, exchang);
    #endif // ENABLE_DEBUG_LOGS
    for(size_t i = 0; i < params.size(); i += BATCH_SIZE){
    size_t end = std::min(i + BATCH_SIZE, params.size());
    json::array params_batch;
    for(int j = i; j < end; ++j){
        params_batch.emplace_back(params[j]);
    }
    json::object req;
    req["method"] = "SUBSCRIBE";
    req["params"] = params_batch;
    req["id"] = this->id_++;
    std::string msg = json::serialize(req);
    co_await this->wss_.async_write(net::buffer(msg), net::use_awaitable);
    net::steady_timer timer(ioc_);
    co_await this->do_read();
    timer.expires_after(std::chrono::milliseconds(300));
    co_await timer.async_wait(net::use_awaitable);
    }
};

net::awaitable<void> wssBinanceClient::do_names(){
    try{
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_map running", exchang);
    #endif // ENABLE_DEBUG_LOGS
    net::ip::tcp::resolver resolver_(ioc_);
    auto result = co_await resolver_.async_resolve("api.binance.com", "443", net::use_awaitable);
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
    if(!SSL_set_tlsext_host_name(stream.native_handle(), "api.binance.com")){
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
    req.target("/api/v3/exchangeInfo?permissions=SPOT");
    req.version(12);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::host, "api.binance.com");
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
        if(vo.contains("symbols")){
            json::array symbols = vo["symbols"].as_array();
            #ifdef ENABLE_DEBUG_LOGS
                Log_Debug("Parsing...", exchang);
            #endif // ENABLE_DEBUG_LOGS
            for(auto& item : symbols){
                if(item.is_object()){
                    auto& obj = item.as_object();
                    if(obj.contains("symbol") && obj.contains("status") 
                    && obj.contains("isSpotTradingAllowed")){
                        std::string status = obj["status"].as_string().c_str();
                        bool isSpot = obj["isSpotTradingAllowed"].as_bool();
                        if(status == "TRADING" && isSpot){
                        if(obj["symbol"].as_string().find("USDT") != std::string::npos){
                            pairs.push_back(obj["symbol"].as_string().c_str());
                        }
                    }
                    }
                }
            }
            #ifdef ENABLE_DEBUG_LOGS
                Log_Debug("Parsing success", exchang);
            #endif // ENABLE_DEBUG_LOGS
        }
    }
    }catch(const std::exception& exc){
        Log_Critical(exc.what(), exchang);
    };
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_map ending", exchang);
    #endif // ENABLE_DEBUG_LOGS
};


void wssBinanceClient::parser(json::value&& jv) {
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu parser start", exchang);
    #endif // ENABLE_DEBUG_LOGS
    try{
       if(jv.is_object()){
        auto object = jv.as_object();
            if(object.contains("stream")){
                if(object["stream"].as_string().find("bookTicker") != std::string::npos){
                    if(object.contains("data") && object["data"].is_object()){
                        auto it = map_.find(object["data"].as_object()["s"].as_string().c_str());
                        if(it != map_.end()){
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
                        t.a = get_double("a");
                        t.b = get_double("b");
                        t.qty_a = get_double("A");
                        t.qty_b = get_double("B");
                        net_->push_data(std::move(t), exchang, it->second);
                        }
                    }
                }else if(object["stream"].as_string().find("depth20") != std::string::npos){
                    if(object.contains("data") && object["data"].is_object()){
                        auto normalize = [this](std::string&& str) -> std::optional<std::string> {
                            try{
                            char delimiter = '@';
                            auto it = str.find(delimiter);
                            if(it != std::string::npos){
                                str.erase(it);
                            }
                            std::transform(str.begin(), str.end(), str.begin(),[](char c){return std::toupper(c);});
                            return std::optional<std::string>{str};
                            }catch(const std::exception& exc){
                                Log_Warn("Fu normalize exc. Delimiter not found", this->exchang);
                                return std::nullopt;
                            };
                        };
                    auto result = normalize(std::move(object["stream"].as_string().c_str()));
                    size_t index = -1;
                    if(result != std::nullopt){
                        auto it = map_.find(result.value());
                        if(it != map_.end()){
                            index = it->second;
                        }else{return;}
                    }else{return;}
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
                }
            }
       }
       if(jv.is_array()){
            
       }
    }catch(const std::exception& exc){
        std::string msg = "Error parser: ";
        Log_Critical(msg + exc.what(), exchang);
    }   
}