cmake_minimum_required(VERSION 2.8)

SET (AC_SRC
  algorithm/configuration.cc
  algorithm/paxos.cc
  algorithm/paxos_server.cc
  algorithm/paxos_log_cache.cc
  algorithm/paxos_error.cc
  algorithm/msg_compress.cc
  algorithm/crc.cc
  algorithm/log_meta_cache.cc
  protocol/paxos.pb.cc
  net/easyNet.cc
  service/service.cc
  service/thread_timer.cc
  service/client_service.cc
  )

SET (INC_FILE
  include/net.h
  include/paxos_server.h
  include/client_service.h
  include/paxos_configuration.h
  include/paxos.h
  include/paxos_log.h
  include/consensus.h
  include/easyNet.h
  include/service.h
  include/thread_timer.h
  include/single_process_queue.h
  include/paxos_log_cache.h
  include/paxos_error.h
  include/msg_compress.h
  include/crc.h
  include/log_meta_cache.h
  include/paxos_option.h
  protocol/paxos.pb.h
  )

SET (INC_FILE_SDK
  protocol/paxos.pb.h
  )

OPTION(WITH_DEBUG "Debug Mode" ON)
OPTION(WITH_DEBUG_LOG_CACHE "Debug Mode For Log Cache" OFF)
OPTION(WITH_TSAN "ThreadSanitizer Mode" OFF)
OPTION(WITH_ASAN "AddressSanitizer Mode" OFF)
OPTION(WITH_GPT "GooglePerfTools Enable" OFF)
OPTION(WITHOUT_CXX11_ABI "Build with -D_GLIBCXX_USE_CXX11_ABI=0" OFF)

IF(WITH_TSAN)
  MESSAGE(STATUS "Build with ThreadSanitizer Enable!")
  SET (EXTRA_SAN_FLAG -fsanitize=thread)
ENDIF()

IF(WITH_ASAN)
  MESSAGE(STATUS "Build with AddressSanitizer Enable!")
  SET (EXTRA_SAN_FLAG "-fsanitize=address")
ENDIF()

IF(WITHOUT_CXX11_ABI)
	MESSAGE(STATUS "Build without CXX11_ABI!")
	SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
ENDIF()

IF(WITH_GPT)
  MESSAGE(STATUS "Build with GooglePerfTools Enable!")
  SET (EXTRA_LINK tcmalloc_and_profiler)
ENDIF()

IF(WITH_DEBUG)
  MESSAGE(STATUS "Build with debug mode!")
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O0 -fno-omit-frame-pointer -fPIC -fno-strict-aliasing ${EXTRA_SAN_FLAG}") 
ELSE() 
  MESSAGE(STATUS "Build with release mode!")
  SET(CMAKE_BUILD_TYPE "RelWithDebInfo")
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O3 -fPIC ")
ENDIF()

include_directories(include
  ${CMAKE_INSTALL_PREFIX}/include/easy
  ${CMAKE_CURRENT_SOURCE_DIR}/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/io/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/util/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/thread/
  )

SET (LIBEASY_OUTPUT
  ${CMAKE_INSTALL_PREFIX}/lib/libmyeasy.a
  )
IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
add_custom_command (
  OUTPUT ${LIBEASY_OUTPUT}
  COMMAND [ -d output ] && mkdir output || echo "create output"
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src && ([ -d
  bu ] || (mkdir bu && cd bu && cmake -D
  CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} .. && cd ..) ) && cd
  bu && make -j && make install
  COMMENT "building libmyeasy"
)
ELSE()
add_custom_command (
  OUTPUT ${LIBEASY_OUTPUT}
  COMMAND [ -d output ] && mkdir output || echo "create output"
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src && ([ -d
  bu ] || (mkdir bu && cd bu && cmake -D
  CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -D WITH_ASAN=${WITH_ASAN} -DEASY_SPIN_USE_SYS=1 .. && cd ..) ) && cd
  bu && make -j && make install
  COMMENT "building libmyeasy"
)
ENDIF()
add_custom_target (libmyeasy
  ALL DEPENDS ${LIBEASY_OUTPUT}
  )

link_directories(
  ${CMAKE_INSTALL_PREFIX}/lib
  )

find_package(Protobuf REQUIRED)
SET (PROTOBUF_CPP ${CMAKE_CURRENT_SOURCE_DIR}/consensus/protocol/paxos.pb.c ${CMAKE_CURRENT_SOURCE_DIR}/consensus/protocol/paxos.pb.h)
add_custom_command (
  OUTPUT ${PROTOBUF_CPP}
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/protocol &&
  (${PROTOBUF_PROTOC_EXECUTABLE} -I. --cpp_out=. paxos.proto)
  COMMENT "generage protobuf cpp"
)
add_custom_target (paxos_pb
  ALL DEPENDS ${PROTOBUF_CPP}
)
include_directories(${Protobuf_INCLUDE_DIRS})
add_definitions(-w)
add_library(aliconsensus STATIC ${AC_SRC})
target_link_libraries(aliconsensus pthread snappy z
  	bz2 lz4 rt ssl crypto myeasy ${PROTOBUF_LIBRARIES} ${EXTRA_LINK})
add_dependencies(aliconsensus paxos_pb)

install(TARGETS aliconsensus DESTINATION lib)
install(FILES ${INC_FILE} DESTINATION include/aliconsensus)

set( CMAKE_EXPORT_COMPILE_COMMANDS 1 )
