#include"net.hpp"

std::string Net::convert(std::string str){
str.erase(std::remove_if(str.begin(), str.end(), [](char c) {
            return c == '-' || c == '_' || c == '/';
        }), str.end());
std::transform(str.begin(), str.end(), str.begin(), ::toupper);
return str;
}

net::awaitable<void> Net::build_maping(){
    for(auto& it : clients){
        co_await it->do_names();
    }
    size_t global_index = 0;
    for(auto& it : clients){
        for(auto& name : it->get_pairs()){
            if(name.find("USDT") != std::string::npos){
            std::string name_temp = convert(name);
            auto [it_map, inserted] = global_map.emplace(name_temp, global_index);
            if(inserted){
                token_map.push_back(name_temp);
                ++global_index;
            }
        }
        }
    }
};

void Net::push_data(wssClient::Token&& token, Logger::Exchanges exch, size_t index){
    data[index][static_cast<int>(exch)].store(token, std::memory_order_release);
};

void Net::set_maping_local(){
    for(auto& it : clients){
        std::unordered_map<std::string, size_t> local_map;
        for(auto& name : it->get_pairs()){
            auto it = global_map.find(convert(name));
            if(it != global_map.end()){
                size_t index = it->second;
                local_map.emplace(name, index);
            }
        }
        it->set_map(std::move(local_map));
    }
};

Net::Net(): ioc() {
clients.resize(0);
clients.reserve(30);
auto ioc_1 = std::make_unique<net::io_context>();
clients.push_back(std::make_unique<wssBinanceClient>(*ioc_1, "stream.binance.com","","9443","/stream",
    Logger::Exchanges::BINANCE, this));
clients_ioc_.push_back(std::move(ioc_1));

auto ioc_2 = std::make_unique<net::io_context>();
clients.push_back(std::make_unique<wssKucoinClient>(*ioc_2, "","","443","",
    Logger::Exchanges::KUCOIN, this));
clients_ioc_.push_back(std::move(ioc_2));


};

void Net::Start(){
            #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("Net::Start: START", Logger::Exchanges::UNKNOW);
            #endif // ENABLE_DEBUG_LOGS
    for (size_t i = 0; i < clients_ioc_.size(); ++i) {
        threads_.emplace_back([this, i]() {
            #ifdef ENABLE_DEBUG_LOGS
                std::string msg = "Thread for client " + std::to_string(i) + " started";
                Log_Debug(msg, Logger::Exchanges::UNKNOW);
            #endif // ENABLE_DEBUG_LOGS
            auto guard = net::make_work_guard(*clients_ioc_[i]);
            clients_ioc_[i]->run();
            #ifdef ENABLE_DEBUG_LOGS
                msg = "Thread for client " + std::to_string(i) + " stopped";
            Log_Debug(msg, Logger::Exchanges::UNKNOW);
            #endif // ENABLE_DEBUG_LOGS
        });
    }
            #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("Net::Start: all threads launched", Logger::Exchanges::UNKNOW);
            #endif // ENABLE_DEBUG_LOGS
    std::promise<void> promise;
    auto future = promise.get_future();
            #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("Net::Start: before co_spawn build_maping", Logger::Exchanges::UNKNOW);
            #endif // ENABLE_DEBUG_LOGS
    net::co_spawn(*clients_ioc_[0], build_maping(), [&promise, this](std::exception_ptr exc){
        #ifdef ENABLE_DEBUG_LOGS
            Log_Debug("build_maping completion handler called", Logger::Exchanges::UNKNOW);
        #endif // ENABLE_DEBUG_LOGS
        if(exc){std::rethrow_exception(exc);}
        promise.set_value();
    });
    future.wait();
    set_maping_local();
    data.resize(global_map.size(), clients.size());
    deep_data.resize(20, clients.size(), global_map.size());
    
    for(int i = 0; i < clients.size(); ++i){
        clients[i]->set_global_size_(global_map.size());
        net::co_spawn(*clients_ioc_[i], clients[i]->run_forever(), net::detached);
    }
    for(auto& t : threads_){
        t.join();
    }
};

void Net::push_deep(std::pair<DepthData::Level, DepthData::Level> data_de, size_t level, size_t col, size_t row){
    deep_data.bids_emplace(level, col, row, data_de.first);
    deep_data.asks_emplace(level, col, row, data_de.second);
}

