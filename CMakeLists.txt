cmake_minimum_required(VERSION 3.20)
project(agregator_)

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
    boost
    URL https://archives.boost.io/release/1.91.0/source/boost_1_91_0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(boost)

find_package(OpenSSL REQUIRED)

find_package(Boost REQUIRED COMPONENTS json)

set(NETWORK_SOURCE
    ${CMAKE_SOURCE_DIR}/src/network/net.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/net_utils.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/binance_client.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/kucoin_client.cpp
)

set(NETWORK_TARGET
    ${CMAKE_SOURCE_DIR}/include/agregator_
    ${CMAKE_SOURCE_DIR}/src/network
    ${CMAKE_SOURCE_DIR}/src/network/utils
    )

add_library(logger STATIC ${CMAKE_SOURCE_DIR}/src/logger/logger.cpp)
target_include_directories(logger PUBLIC ${CMAKE_SOURCE_DIR}/include/agregator_)


add_library(net STATIC ${NETWORK_SOURCE})
target_include_directories(net PUBLIC ${NETWORK_TARGET})
target_link_libraries(net PRIVATE OpenSSL::SSL OpenSSL::Crypto logger Boost::json atomic)

add_library(agr STATIC ${CMAKE_SOURCE_DIR}/src/agregator/agr.cpp)
target_include_directories(agr PUBLIC
 ${CMAKE_SOURCE_DIR}/include/agregator_ ${CMAKE_SOURCE_DIR}/src/agregator ${NETWORK_TARGET})

set(MYLIB_SOURCE
    ${CMAKE_SOURCE_DIR}/src/network/net.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/net_utils.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/binance_client.cpp
    ${CMAKE_SOURCE_DIR}/src/logger/logger.cpp
    ${CMAKE_SOURCE_DIR}/src/agregator/agr.cpp
    ${CMAKE_SOURCE_DIR}/src/network/utils/kucoin_client.cpp
)
set(MYLIB_TARGET 
    ${CMAKE_SOURCE_DIR}/include/agregator_
    ${CMAKE_SOURCE_DIR}/src/network
    ${CMAKE_SOURCE_DIR}/src/network/utils
    ${CMAKE_SOURCE_DIR}/src/agregator
)

option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
if(ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -g -O1 -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

#add_compile_definitions(ENABLE_DEBUG_LOGS)

add_library(MyLib STATIC ${MYLIB_SOURCE})
target_include_directories(MyLib PUBLIC ${MYLIB_TARGET})
target_include_directories(MyLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(main ${CMAKE_SOURCE_DIR}/src/main.cpp)
target_link_libraries(main PRIVATE MyLib OpenSSL::SSL OpenSSL::Crypto Boost::json atomic)


if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-fcoroutines)
endif()

option(BUILD_TESTS "Build tests" ON)
