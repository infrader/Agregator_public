#include"net_utils.hpp"


net::awaitable<void> wssClient::do_connect(){
    auto& lowest = beast::get_lowest_layer(this->ssl_stream_);
    lowest.close(); 
    SSL_clear(this->ssl_stream_.native_handle());
    try{
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_connect running", exchang);
    #endif // ENABLE_DEBUG_LOGS
    auto executor_ = co_await net::this_coro::executor;
    tcp::resolver resolver_(executor_);
    auto results = co_await resolver_.async_resolve(this->host_, this->port_, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Resolver success resolve()", exchang);
    #endif // ENABLE_DEBUG_LOGS
    ssl::context ctx(ssl::context_base::tlsv12_client);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL context params set success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    co_await beast::get_lowest_layer(this->ssl_stream_).async_connect(results, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL stream do connect success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    if(!SSL_set_tlsext_host_name(this->ssl_stream_.native_handle(), this->host_.c_str())){
        throw std::runtime_error{"SNI failure"};
    }
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SNI success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    co_await this->ssl_stream_.async_handshake(ssl::stream_base::client, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("SSL handshake success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    co_await this->wss_.async_handshake(this->host_, this->target_, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Websocket handshake success", exchang);
    #endif // ENABLE_DEBUG_LOGS
    }catch(const std::exception& exc){
        std::string msg = "Error in fu do_connect(), ";
        Log_Warn(msg + exc.what(), exchang);
        throw exc;
    }
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_connect has ended", exchang);
    #endif // ENABLE_DEBUG_LOGS
};

net::awaitable<void> wssClient::run_forever(){
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("run_forever: entered", exchang);
    #endif // ENABLE_DEBUG_LOGS
    reconnect.store(false, std::memory_order_release);
    while(true){
    #ifdef ENABLE_DEBUG_LOGS
        if(reconnect.load(std::memory_order_acquire)){
            Log_Debug("Field reconnect = true!", exchang);
        }else{
            Log_Debug("Field reconnect = false!", exchang);
        }
        Log_Debug("Run cycle connect && read", exchang);
    #endif // ENABLE_DEBUG_LOGS
        try{
            co_await do_connect();
            co_await do_subscribe();
            #ifdef ENABLE_DEBUG_LOGS
            unsigned long long sz{0};
            #endif // ENABLE_DEBUG_LOGS
            while(true){
            #ifdef ENABLE_DEBUG_LOGS
            std::string sz_str = std::to_string(sz++);
               Log_Debug("SIZE read = " + sz_str, exchang);
            #endif // ENABLE_DEBUG_LOGS
                co_await do_read();
            }
        }catch(const std::exception& exc){
            std::string str = " reconnection...";
            Log_Critical(exc.what() + str, exchang);
            reconnect.store(true, std::memory_order_release);
        }
    if(reconnect.load(std::memory_order_acquire)){
        #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("Try reconnect...", exchang);
        #endif // ENABLE_DEBUG_LOGS
            net::steady_timer timer(ioc_);
            timer.expires_after(std::chrono::seconds(5));
            co_await timer.async_wait(net::use_awaitable);
        }
        else{
        std::string msg{"Field reconnect isn't true! Reconnect without timer."};
        std::string msg2{"Infinite looping!"};
        Log_Critical(msg , exchang);
        Log_Warn(msg2, exchang);
        #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("Program stopted without reconnect!", exchang);
        #endif // ENABLE_DEBUG_LOGS
        }
        reconnect.store(false, std::memory_order_release);
    };
}
void wssClient::set_map(std::unordered_map<std::string, size_t>&& map){
    map_ = std::move(map);
}

 std::vector<std::string>& wssClient::get_pairs(){
    return this->pairs;
 };

net::awaitable<void> wssClient::do_read(){
    #ifdef ENABLE_DEBUG_LOGS
        Log_Debug("Fu do_read() run!", exchang);
    #endif // ENABLE_DEBUG_LOGS
    try{
    size_t byte = co_await this->wss_.async_read(this->buff_, net::use_awaitable);
    #ifdef ENABLE_DEBUG_LOGS
        std::string str_byte = std::to_string(byte);
        Log_Debug("Buffer size read: " + str_byte + ".", exchang);
    #endif // ENABLE_DEBUG_LOGS
    parser(json::parse(beast::buffers_to_string(buff_.data()))); 
    this->buff_.consume(byte);
    #ifdef ENABLE_DEBUG_LOGS
        str_byte = std::to_string(buff_.size());
        Log_Debug("Buffer size after consume: " + str_byte + ".", exchang);
    #endif // ENABLE_DEBUG_LOGS
    }catch(const std::exception& exc){
        std::string msg{" !Exceprion in fu do_read()!"};
        Log_Critical(exc.what() + msg, exchang);
        throw exc;
    }
};

void wssClient::set_global_size_(size_t&& size){
    global_size_ = std::move(size);
};
