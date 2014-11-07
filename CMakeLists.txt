cmake_minimum_required(VERSION 2.8.3)
project(mongodb_log)

find_package(catkin REQUIRED COMPONENTS
  roscpp roslib designator_integration designator_integration_msgs)

catkin_package(
  INCLUDE_DIRS ${catkin_INCLUDE_DIRS}
  LIBRARIES
  CATKIN_DEPENDS roscpp roslib designator_integration designator_integration_msgs
)

include_directories(
  ${catkin_INCLUDE_DIRS})

add_executable(mongodb_log_tf src/mongodb_log_tf.cpp)
add_executable(mongodb_log_pcl src/mongodb_log_pcl.cpp)
add_executable(mongodb_log_cimg src/mongodb_log_cimg.cpp)
add_executable(mongodb_log_desig src/mongodb_log_desig.cpp)
add_executable(benchmark_produce_tf src/benchmark/produce_tf.cpp)

add_dependencies(mongodb_log_desig designator_integration_generate_messages_cpp)

target_link_libraries(mongodb_log_tf
  ${catkin_LIBRARIES}
  # NOTE(winkler): Yes, this looks awkward, and plain wrong. One might
  # suspect that the two lines actually mean the same, as their paths
  # resolve to the same file. If one of them is removed, though, boost
  # is not linked properly anymore. When cmake'ing, CMake complains
  # that one might shadow the other. Still need to figure out why this
  # happens, but this hack now works, and seems to be at least
  # semi-portable between different Ubuntu versions. If you have a
  # clue and are a CMake guru: Please have a look! The culprit might
  # be `mongoclient'.
  "/usr/lib/x86_64-linux-gnu/libboost_filesystem.so"
  "/usr/lib/gcc/../x86_64-linux-gnu/libboost_filesystem.so"
  ssl
  crypto
  mongoclient
)

target_link_libraries(mongodb_log_pcl
  ${catkin_LIBRARIES}
  "/usr/lib/x86_64-linux-gnu/libboost_filesystem.so"
  "/usr/lib/gcc/../x86_64-linux-gnu/libboost_filesystem.so"
  ssl
  crypto
  mongoclient
)

target_link_libraries(mongodb_log_cimg
  ${catkin_LIBRARIES}
  "/usr/lib/x86_64-linux-gnu/libboost_filesystem.so"
  "/usr/lib/gcc/../x86_64-linux-gnu/libboost_filesystem.so"
  ssl
  crypto
  mongoclient
)

target_link_libraries(mongodb_log_desig
  ${catkin_LIBRARIES}
  "/usr/lib/x86_64-linux-gnu/libboost_filesystem.so"
  "/usr/lib/gcc/../x86_64-linux-gnu/libboost_filesystem.so"
  ssl
  crypto
  mongoclient
)

target_link_libraries(benchmark_produce_tf
  ${catkin_LIBRARIES}
  "/usr/lib/x86_64-linux-gnu/libboost_filesystem.so"
  "/usr/lib/gcc/../x86_64-linux-gnu/libboost_filesystem.so"
  ssl
  crypto
  mongoclient
)
