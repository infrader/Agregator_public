#pragma once
#define LOGGER_HPP_
#ifdef LOGGER_HPP_

#include<string>
#include<atomic>
#include <string_view>
#include<iostream>
#include<fstream>
#include<format>
#include<chrono>
#include<vector>
#include<queue>
#include<algorithm>
#include<thread>
#include<condition_variable>
#include<mutex>

#define Log_Intence Logger::getInstance
#define Log_setCurrenLevel(currentLevel) Logger::getInstance()->setCurrenLevel(currentLevel)
#define Log_Info(msg, exchang) Logger::getInstance()->info(msg, exchang)
#define Log_Trace(msg, exchang) Logger::getInstance()->trace(msg, exchang)
#define Log_Debug(msg, exchang) Logger::getInstance()->debug(msg, exchang)
#define Log_Critical(msg, exchang) Logger::getInstance()->critical(msg, exchang)
#define Log_Warn(msg, exchang) Logger::getInstance()->warn(msg, exchang)
#define Log_SetLoggerFile(msg) Logger::getInstance()->setLogger_file(msg)
#define Log_ExchangeToStr(exchang) Logger::getInstance()->ExchangeToStr(exchang)



class Logger {
public:
	enum class State_Level { TRACE,DEBUG,INFO,WARN,CRITICAL};
	enum class Exchanges {BINANCE,KUCOIN,UNKNOW};
	static Logger* getInstance();
	 Logger(const Logger&) = delete;
	 Logger& operator=(const Logger&) = delete;
	 void setLogger_file(std::string msg);
	 void info(const std::string& msg, Exchanges exchang);
	 void trace(const std::string& msg, Exchanges exchang);
	 void debug(const std::string& msg, Exchanges exchang);
	 void critical(const std::string& msg, Exchanges exchang);
	 void warn(const std::string& msg, Exchanges exchang);
	 void setCurrenLevel(State_Level lvl);
	 std::string ExchangeToStr(Exchanges exchang);
	 ~Logger();
private:
	
	template<class T>
struct queue_log{
	public:
	inline void push(T msg){
		std::lock_guard<std::mutex> guard(mtx);
		qu_.push(msg);
		cv.notify_all();
	};
	inline T front(){
		std::lock_guard<std::mutex> guard(mtx);
		auto t = qu_.front();
		qu_.pop();
		return t;
	}
	inline void notify_all(){
		cv.notify_all();
	}
	inline void pop(){
		std::lock_guard<std::mutex> guard(mtx);
		qu_.pop();
	}
	inline bool wait(std::atomic<bool>& stop_read){
		std::unique_lock<std::mutex> lock(mtx);
		cv.wait(lock, [this, &stop_read]{return !qu_.empty() || stop_read.load(std::memory_order_acquire);});
		if(stop_read.load(std::memory_order_acquire)){return false;}
		return true;
	};
	private:
	std::atomic<bool> stop_read;
	std::queue<T> qu_;
	std::mutex mtx;
	std::condition_variable cv;
};
	struct Message{
		State_Level level_;
		const std::string msg_;
		Exchanges exchang_;
		Message(State_Level level, std::string msg, Exchanges exchang)
		 : level_(level), msg_(std::move(msg)), exchang_(exchang){};
	};
	std::ofstream logger_file;
	std::string nameFile;
	void on_read();
	std::atomic<bool> stop_read;
	queue_log<Message> msg_qu;
	State_Level currentLevel;
	Logger():currentLevel(State_Level::CRITICAL), nameFile("LogFile"),  
	th([this](){logger_file.open(nameFile, std::ofstream::app); on_read();}), stop_read(false){}
	void log();
	std::string getCurrentTime();
	const std::string& LeveltoString(State_Level level);
	std::thread th;
};



#endif // LOGGER_HPP_