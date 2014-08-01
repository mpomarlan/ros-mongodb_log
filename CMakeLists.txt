cmake_minimum_required(VERSION 2.8.3)
project(mongodb_log)

find_package(catkin REQUIRED COMPONENTS
  roscpp roslib designator_integration designator_integration_msgs)
find_package(Boost REQUIRED COMPONENTS
  thread system filesystem)

catkin_package(
  INCLUDE_DIRS
  LIBRARIES
  CATKIN_DEPENDS roscpp roslib designator_integration designator_integration_msgs
)

include_directories(include ${Boost_INCLUDE_DIRS} ${catkin_INCLUDE_DIRS})

add_executable(mongodb_log_tf src/mongodb_log_tf.cpp)
add_executable(mongodb_log_pcl src/mongodb_log_pcl.cpp)
add_executable(mongodb_log_cimg src/mongodb_log_cimg.cpp)
add_executable(mongodb_log_desig src/mongodb_log_desig.cpp)
add_executable(benchmark_produce_tf src/benchmark/produce_tf.cpp)

add_dependencies(mongodb_log_desig designator_integration_generate_messages_cpp)

link_directories(${Boost_LIBRARY_DIRS})

target_link_libraries(mongodb_log_tf
  ${catkin_LIBRARIES}
  ${Boost_LIBRARIES}
  # NOTE(winkler): This is a dirty, dirty workaround as boost seems to
  # be found in the wrong place (failing linking). Hopefully, these
  # files are present on your system. If they are not, linking will
  # fail definitely. Resolving the slashed paths into
  # `/usr/lib/libboost_filesystem-mt.so` somehow fails as if the whole
  # thing wasn't there, which is pretty weird. Will try to fix this
  # ASAP.
  /usr/lib/gcc/x86_64-linux-gnu/4.7/../../../../lib/libboost_filesystem-mt.so
  mongoclient
)

target_link_libraries(mongodb_log_pcl
  ${catkin_LIBRARIES}
  ${Boost_LIBRARIES}
  /usr/lib/gcc/x86_64-linux-gnu/4.7/../../../../lib/libboost_filesystem-mt.so
  mongoclient
)

target_link_libraries(mongodb_log_cimg
  ${catkin_LIBRARIES}
  ${Boost_LIBRARIES}
  /usr/lib/gcc/x86_64-linux-gnu/4.7/../../../../lib/libboost_filesystem-mt.so
  mongoclient
)

target_link_libraries(mongodb_log_desig
  ${catkin_LIBRARIES}
  ${Boost_LIBRARIES}
  /usr/lib/gcc/x86_64-linux-gnu/4.7/../../../../lib/libboost_filesystem-mt.so
  mongoclient
)

target_link_libraries(benchmark_produce_tf
  ${catkin_LIBRARIES}
  ${Boost_LIBRARIES}
  /usr/lib/gcc/x86_64-linux-gnu/4.7/../../../../lib/libboost_filesystem-mt.so
  mongoclient
)
