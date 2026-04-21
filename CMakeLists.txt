cmake_minimum_required(VERSION 3.20)
project(MaigentQueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
find_path(NATS_INCLUDE_DIR nats/nats.h REQUIRED)
find_library(NATS_LIBRARY nats REQUIRED)

execute_process(
  COMMAND pkg-config --libs protobuf
  OUTPUT_VARIABLE PROTOBUF_PKG_LIBS_RAW
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
separate_arguments(PROTOBUF_PKG_LIBS_RAW)

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

add_library(maigent_proto STATIC ${GEN_DIR}/maigent.pb.cc)
target_include_directories(maigent_proto PUBLIC ${GEN_DIR})
target_link_libraries(maigent_proto PUBLIC protobuf::libprotobuf
                                         ${PROTOBUF_PKG_LIBS_RAW})

target_compile_features(maigent_proto PUBLIC cxx_std_20)

add_library(ma_common STATIC
  src/common/time_utils.cpp
  src/common/ids.cpp
  src/common/config.cpp
  src/common/logging.cpp
  src/common/target_model.cpp
  src/common/target_model_proto.cpp
  src/common/nats_wrapper.cpp
  src/common/message_helpers.cpp
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
    maigent_proto
    ${NATS_LIBRARY}
    pthread
)

target_compile_features(ma_common PUBLIC cxx_std_20)

add_library(system_monitor_core STATIC
  src/system_monitor/host_raw_collector.cpp
  src/system_monitor/raw_collector.cpp
  src/system_monitor/feature_builder.cpp
  src/system_monitor/model_contract.cpp
  src/system_monitor/target_classifier_heuristic.cpp
  src/system_monitor/system_monitor_model_heuristic.cpp
)
target_link_libraries(system_monitor_core PUBLIC ma_common)
target_compile_features(system_monitor_core PUBLIC cxx_std_20)

add_library(planner_core STATIC
  src/planner/model_contract.cpp
  src/planner/policy_engine_heuristic.cpp
)
target_link_libraries(planner_core PUBLIC ma_common)
target_compile_features(planner_core PUBLIC cxx_std_20)

add_executable(task_manager_agent src/task_manager/task_manager_agent.cpp)
target_link_libraries(task_manager_agent PRIVATE ma_common)

add_executable(lease_authority_agent src/lease_authority/lease_authority_agent.cpp)
target_link_libraries(lease_authority_agent PRIVATE ma_common)

add_executable(system_monitor_agent src/system_monitor/system_monitor_agent.cpp)
target_link_libraries(system_monitor_agent PRIVATE ma_common system_monitor_core)

add_executable(planner_agent src/planner/planner_agent.cpp)
target_link_libraries(planner_agent PRIVATE ma_common planner_core)

add_executable(task_executor_agent src/task_executor/task_executor_agent.cpp)
target_link_libraries(task_executor_agent PRIVATE ma_common)

add_executable(actuator_agent src/actuator/actuator_agent.cpp)
target_link_libraries(actuator_agent PRIVATE ma_common)

add_executable(directory_agent src/directory/directory_agent.cpp)
target_link_libraries(directory_agent PRIVATE ma_common)

add_executable(metrics_collector_agent src/metrics_collector/metrics_collector_agent.cpp)
target_link_libraries(metrics_collector_agent PRIVATE ma_common)

add_executable(bench_submitter tools/bench_submitter.cpp)
target_link_libraries(bench_submitter PRIVATE ma_common)

add_executable(system_status_client tools/system_status_client.cpp)
target_link_libraries(system_status_client PRIVATE ma_common)
