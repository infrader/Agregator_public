#include"agr.hpp"


void Agregator::find_divergence(){
    for(size_t i = 0; i < data.rows_size(); ++i){
        double min = std::numeric_limits<double>::max();
        double max = std::numeric_limits<double>::min();
        std::pair<size_t, size_t> pair;
        for(size_t j = 0; j < data.cols_size(); ++j){
            auto temp = data[i][j].load(std::memory_order_acquire);
            if (temp.a <= 0 || temp.b <= 0) continue;
            if(min > temp.a){min = temp.a;pair.first = j;}
            if(max < temp.b){max = temp.b;pair.second = j;}
        }
        if(pair.first != pair.second){
        if(((max - min) / max) * 100 > 2){
            std::lock_guard<std::mutex> lock(mtx);
            qu_work.push({i,pair});
            cv.notify_all();
        }
    }
    }
};
       

void Agregator::discrepancy_handlers(){
    while(true){
        std::pair<size_t, std::pair<size_t,size_t>> pair;
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this]{return !qu_work.empty() || !divergence_work.load(std::memory_order_acquire);});
            if(qu_work.empty() || !divergence_work.load(std::memory_order_acquire)){
                break;
            }
            pair = qu_work.front();
            qu_work.pop();
        }
        size_t token = pair.first;
        size_t min = pair.second.first;
        size_t max = pair.second.second;
        if(work.find(token) != work.end()){
            if(!work[token].load(std::memory_order_acquire)){
                work[token].store(true,std::memory_order_release);
                auto now = std::chrono::steady_clock::now() + std::chrono::minutes(5);

            }
        }
    }
};