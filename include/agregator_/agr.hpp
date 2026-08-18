#include<logger.hpp>
#define AGR_HPP
#ifdef AGR_HPP
#include<thread>
#include"net.hpp"
#include<atomic>
#include<limits>
#include<mutex>

class Agregator{
    public:
    Agregator(Net& net): net_(net), 
        global_map(net.get_glmap()),
          data(net.get_data()),
          deep_data(net.get_dpdata()), token_map(net.get_tkmap()){
            divergence_work.store(true,std::memory_order_acquire);
            thread_pool.push_back(std::thread([this](){
                while(data.rows_size() == 0 || data.cols_size() == 0 || token_map.empty() || global_map.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                while(divergence_work.load(std::memory_order_acquire)){
                    find_divergence();
                }
            }));
            for(size_t i = 0; i < 4; ++i){
                thread_pool.push_back(std::thread([this](){
                while(divergence_work.load(std::memory_order_acquire)){
                    discrepancy_handlers();
                }
            }));
            }
          }
    ~Agregator(){
        divergence_work.store(false, std::memory_order_release);
        cv.notify_all();
        for(auto& t: thread_pool){
            if(t.joinable()){
                t.join();
            }
        }
    }
    private:
        double spred = 5;
        std::condition_variable cv;
        std::mutex mtx;
        std::atomic<bool> divergence_work;
        void find_divergence();
        void discrepancy_handlers();
        std::vector<std::thread> thread_pool;
        std::queue<std::pair<size_t, std::pair<size_t, size_t>>> qu_work;
        Net& net_;
        std::unordered_map<std::string, size_t>& global_map;
        std::vector<std::string>& token_map;
        std::unordered_map<size_t, std::atomic<bool>> work;
        AtomicMatrix<wssClient::Token>& data;
        DepthData& deep_data;
};



#endif // AGR_HPP