#include<logger.hpp>


Logger* Logger::getInstance() {
    static Logger* intance = new Logger;
    return intance;
}
void Logger::setLogger_file(std::string msg) {
    nameFile = msg; 
}

void Logger::info(const std::string& msg, Exchanges exchang)
{
    msg_qu.push(Message{Logger::State_Level::INFO, msg, exchang});
}

void Logger::trace(const std::string& msg, Exchanges exchang)
{
    msg_qu.push(Message{Logger::State_Level::TRACE, msg, exchang});;
}

void Logger::debug(const std::string& msg, Exchanges exchang)
{
    msg_qu.push(Message{Logger::State_Level::DEBUG, msg, exchang});
}

void Logger::critical(const std::string& msg, Exchanges exchang)
{
    msg_qu.push(Message{Logger::State_Level::CRITICAL, msg, exchang});
}

void Logger::warn(const std::string& msg, Exchanges exchang)
{
    msg_qu.push(Message{Logger::State_Level::WARN, msg, exchang});
}

void Logger::setCurrenLevel(State_Level lvl){
    currentLevel = lvl;
}

void Logger::on_read(){
    while (!stop_read.load(std::memory_order_acquire)) {
        if(!msg_qu.wait(stop_read)) break;
        log();
    }
};

Logger::~Logger(){
    stop_read.store(true, std::memory_order_release);
    msg_qu.notify_all();
    if(th.joinable()){
        th.join();
    }
    if(logger_file.is_open()){
    logger_file.flush(); 
    logger_file.close();
    }
}

void Logger::log(){
    Message message_ = msg_qu.front();
    if (static_cast<int>(currentLevel) >= static_cast<int>(message_.level_)) {
        std::string msg_format = std::format("[{}] [{}] [{}] {}",
            Logger::getCurrentTime(),
            Logger::ExchangeToStr(message_.exchang_),
            Logger::LeveltoString(message_.level_),
            message_.msg_);
        try {
            if (logger_file.is_open()) {
                std::string str_info_log = msg_format;
                logger_file << str_info_log << std::endl;
                
                std::cout << msg_format << "\n";
                
            } 
            else {
                std::string error_open = std::format("[{}] [{}] {}", Logger::getCurrentTime(), "File", "Error file isn`t Open");
                std::cout << error_open << "\n";
            };
        }
        catch (...) {
            std::string error_of = std::format("[{}] [{}] {}", Logger::getCurrentTime(), "File", "Error Ofstream");
        }
    }
    else{ return; }
}

std::string Logger::ExchangeToStr(Exchanges exchang){
    static std::pair<int,std::string> exchang_pair[] =
    {
        //BINANCE, KUCOIN
        {0,"BINANCE"},
        {1,"KUCOIN"},
        {2,"UNKNOW"}
    };
    auto result = std::find_if(begin(exchang_pair), end(exchang_pair),
        [exchang](std::pair<int, std::string> x) {return x.first == static_cast<int>(exchang);});
    if (result == end(exchang_pair)) {
        static const std::string unknow = "UNKNOW";
        return unknow;
    }
    return result->second;

}

std::string Logger::getCurrentTime() 
{
    auto now = std::chrono::system_clock::now();
    auto local_time = std::chrono::current_zone()->to_local(now);
    auto time_without_ms = std::chrono::floor<std::chrono::seconds>(local_time);
    std::string view = std::format("{:%H:%M:%S}", time_without_ms);
    return view;
}

const std::string& Logger::LeveltoString(State_Level level) 
{
    static std::pair<int,std::string> level_pair[] =
    {
        {0,"TRACE"},
        {1,"DEBUG"},
        {2,"INFO"},
        {3,"WARN"},
        {4,"CRITICAL"},
    };
    auto result = std::find_if(begin(level_pair), end(level_pair),
        [level](std::pair<int, std::string> x) {return x.first == static_cast<int>(level);});
    if (result == end(level_pair)) {
        static const std::string unknow = "UNKNOW";
        return unknow;
    }
    return result->second;
}