cmake_minimum_required(VERSION 3.14)
project(kafka_client)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(ament_cmake REQUIRED)
find_package(ament_cmake_gtest REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

add_library(${PROJECT_NAME} SHARED
  src/kafka_producer.cpp
)
add_library(${PROJECT_NAME}::${PROJECT_NAME} ALIAS ${PROJECT_NAME})

target_compile_definitions(${PROJECT_NAME}
  PRIVATE "KAFKA_CLIENT_BUILDING_LIBRARY")

target_include_directories(${PROJECT_NAME}
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/${PROJECT_NAME}>
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(${PROJECT_NAME}
  PUBLIC
    ${RDKAFKA_LIBRARIES}
    Threads::Threads
)

install(TARGETS ${PROJECT_NAME}
  EXPORT ${PROJECT_NAME}_export
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

install(DIRECTORY include/
  DESTINATION include/${PROJECT_NAME}
)

ament_export_targets(${PROJECT_NAME}_export HAS_LIBRARY_TARGET)
ament_export_dependencies(Threads)

if(BUILD_TESTING)
  ament_add_gtest(test_kafka_producer
    test/test_kafka_producer.cpp
  )
  target_link_libraries(test_kafka_producer
    ${PROJECT_NAME}
  )

  ament_add_gtest(test_ros_type_validation
    test/test_ros_type_validation.cpp
  )
  target_link_libraries(test_ros_type_validation
    ${PROJECT_NAME}
  )
endif()

ament_package()
