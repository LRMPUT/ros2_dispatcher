cmake_minimum_required(VERSION 3.14)
project(kafka_client)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(ament_cmake REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGETS rdkafka++)

option(KAFKA_CLIENT_BUILD_EXAMPLES "Build kafka_client example executables" OFF)

add_library(kafka_client SHARED
  src/kafka_producer.cpp
)
add_library(${PROJECT_NAME}::${PROJECT_NAME} ALIAS kafka_client)

target_include_directories(kafka_client
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(kafka_client
  PUBLIC
    PkgConfig::RDKAFKA
)

target_compile_definitions(kafka_client
  PRIVATE
    KAFKA_CLIENT_BUILDING_LIBRARY
)

if(KAFKA_CLIENT_BUILD_EXAMPLES)
  add_executable(kafka_client_simple_producer examples/simple_producer.cpp)
  target_link_libraries(kafka_client_simple_producer PRIVATE kafka_client)
  target_include_directories(kafka_client_simple_producer PRIVATE ${RDKAFKA_INCLUDE_DIRS})
  install(TARGETS kafka_client_simple_producer
    RUNTIME DESTINATION lib/${PROJECT_NAME})
endif()

ament_export_targets(export_kafka_client HAS_LIBRARY_TARGET)
ament_export_dependencies(PkgConfig)
ament_export_include_directories(include)
ament_export_libraries(kafka_client)

install(
  DIRECTORY include/
  DESTINATION include
)

install(
  TARGETS kafka_client
  EXPORT export_kafka_client
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include
)

ament_package()
