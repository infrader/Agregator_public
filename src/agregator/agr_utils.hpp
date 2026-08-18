#pragma once
#include"agr.hpp"
#include<queue>


class graph{
    


};

class TimerExpired{
    public:
    struct data_fired{
        std::chrono::steady_clock::time_point time_point;
        size_t token;
        size_t min;
        size_t max;

    };
	inline void push(data_fired data_){
        std::pair<snapshot, snapshot> result = do_snapshot(data_);
		std::lock_guard<std::mutex> guard(mtx);
		qu_.push({data_, result});
        cv.notify_all();
    }
    TimerExpired(DepthData& deep_data, 
        AtomicMatrix<wssClient::Token>& data,
         std::unordered_map<size_t, std::atomic<bool>>& work, std::vector<std::string>& token_map):
         deep_data(deep_data), data(data), work(work), token_map(token_map){
            for(size_t i = 0; i < 2; ++i){
                worker.emplace_back(std::thread([this](){
                    while(fired_work.load(std::memory_order_acquire)){
                    fired();}}));
            }
         };
    ~TimerExpired(){
        fired_work.store(false,std::memory_order_release);
        for(auto& t: worker){
            if(t.joinable()){
                t.join();
            }
        }
    };
    private:
        struct snapshot{
            std::string instrument;
            std::chrono::steady_clock::time_point time_point;
            std::vector<DepthData::Level> lvl_bids;
            std::vector<DepthData::Level> lvl_asks;
            wssClient::Token token_info;
            size_t token;
            size_t exch;
        };
        void fired(){
            while(true){
                snapshot first_min;
                snapshot first_max;
                std::pair<data_fired, std::pair<snapshot, snapshot>> it;
                data_fired data;
                {
                std::unique_lock<std::mutex> guard(mtx);
                cv.wait(guard, [this]{return !qu_.empty();});
                it = qu_.front();
                qu_.pop();
                }
                auto timer = it.first.time_point - std::chrono::steady_clock::now();
                if(timer > std::chrono::nanoseconds(0)){
                std::this_thread::sleep_for(timer);
                }
                auto result_future = do_snapshot(it.first);
                do_write({it.second, result_future});
                work[it.first.token].store(false, std::memory_order_release);
            }
        };
        void do_write(std::pair<std::pair<snapshot, snapshot>, std::pair<snapshot, snapshot>>&& snap){
            //логика записи в файл
        };
        std::pair<snapshot, snapshot> do_snapshot(data_fired data_){
        snapshot first_min;
        snapshot first_max;
        first_min.time_point = std::chrono::steady_clock::now();
        first_min.token_info = data[data_.token][data_.min].load(std::memory_order_acquire);
        first_min.exch = data_.min; first_min.token = data_.token;
        first_min.instrument = token_map[data_.token];
        //
        first_max.time_point = std::chrono::steady_clock::now();
        first_max.token_info = data[data_.token][data_.max].load(std::memory_order_acquire);
        first_max.exch = data_.max; first_max.token = data_.token;
        first_max.instrument = token_map[data_.token];
        //
        for(size_t i = 0; i < 20; ++i){
            first_min.lvl_asks.push_back(deep_data.asks_at(i, data_.min, data_.token));
            first_min.lvl_bids.push_back(deep_data.bids_at(i, data_.min, data_.token));
            first_max.lvl_asks.push_back(deep_data.asks_at(i, data_.max, data_.token));
            first_max.lvl_bids.push_back(deep_data.bids_at(i, data_.max, data_.token));
        }
        return {first_min, first_max};
        }
    std::atomic<bool> fired_work = true;
    std::vector<std::thread> worker;
    std::unordered_map<size_t, std::atomic<bool>>& work;
    std::vector<std::string>& token_map;
    AtomicMatrix<wssClient::Token>& data;
    DepthData& deep_data;
    std::mutex mtx;
    std::queue<std::pair<data_fired, std::pair<snapshot, snapshot>>> qu_;
    std::condition_variable cv;
};