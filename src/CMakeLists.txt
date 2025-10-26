add_executable(manager manager.cpp)
target_link_libraries(manager amqpcpp pthread ev)

add_executable(scheduler scheduler.cpp)
target_link_libraries(scheduler amqpcpp pthread ev)
