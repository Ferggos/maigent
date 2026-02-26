cmake_minimum_required(VERSION 3.20)
project(MasIGentV1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF_PKG REQUIRED IMPORTED_TARGET protobuf)
find_path(NATS_INCLUDE_DIR nats/nats.h REQUIRED)
find_library(NATS_LIBRARY nats REQUIRED)

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/maigent.proto)
set(GEN_DIR ${CMAKE_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})

add_custom_command(
  OUTPUT ${GEN_DIR}/maigent.pb.cc ${GEN_DIR}/maigent.pb.h
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --cpp_out=${GEN_DIR}
          -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating C++ protobuf from maigent.proto"
  VERBATIM
)

add_library(proto_lib STATIC ${GEN_DIR}/maigent.pb.cc)
target_include_directories(proto_lib PUBLIC ${GEN_DIR})
target_link_libraries(proto_lib PUBLIC protobuf::libprotobuf PkgConfig::PROTOBUF_PKG)

target_compile_features(proto_lib PUBLIC cxx_std_20)

add_library(ma_common STATIC
  src/common/logging.cpp
  src/common/utils.cpp
  src/common/message_utils.cpp
  src/common/nats_wrapper.cpp
  src/common/agent_lifecycle.cpp
)
target_include_directories(ma_common
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${GEN_DIR}
    ${NATS_INCLUDE_DIR}
)
target_link_libraries(ma_common
  PUBLIC
    proto_lib
    ${NATS_LIBRARY}
    pthread
)

target_compile_features(ma_common PUBLIC cxx_std_20)

function(add_ma_agent name source)
  add_executable(${name} ${source})
  target_link_libraries(${name} PRIVATE ma_common)
endfunction()

add_ma_agent(directory_agent src/directory_agent.cpp)
add_ma_agent(system_monitor src/system_monitor.cpp)
add_ma_agent(lease_authority src/lease_authority.cpp)
add_ma_agent(executor_agent src/executor_agent.cpp)
add_ma_agent(planner_agent src/planner_agent.cpp)
add_ma_agent(task_manager src/task_manager.cpp)
add_ma_agent(metrics_collector src/metrics_collector.cpp)

add_executable(bench_submitter tools/bench_submitter.cpp)
target_link_libraries(bench_submitter PRIVATE ma_common)

add_executable(system_status_client tools/system_status_client.cpp)
target_link_libraries(system_status_client PRIVATE ma_common)
