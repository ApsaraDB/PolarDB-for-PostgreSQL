cmake_minimum_required(VERSION 2.8)

OPTION(WITH_DEBUG "Debug Mode" ON)
OPTION(WITH_TSAN "ThreadSanitizer Mode" OFF)
OPTION(WITH_ASAN "AddressSanitizer Mode" OFF)
OPTION(WITH_PROTOBUF3 "Build with protobuf3" OFF)
OPTION(WITHOUT_CXX11_ABI "Build with -D_GLIBCXX_USE_CXX11_ABI=0" OFF)

SET (AC_SRC
	polar_consensus_log.cc
	polar_consensus_c.cc
  )

SET (INC_FILE
       include/polar_consensus_c.h
       include/polar_consensus_stats.h
  )


IF(WITH_TSAN)
  MESSAGE(STATUS "Build with ThreadSanitizer Enable!")
  SET (EXTRA_SAN_FLAG -fsanitize=thread)
ENDIF()

IF(WITH_ASAN)
  MESSAGE(STATUS "Build with AddressSanitizer Enable!")
  SET (EXTRA_SAN_FLAG "-fsanitize=address -fno-omit-frame-pointer")
ENDIF()

IF(WITHOUT_CXX11_ABI)
  MESSAGE(STATUS "Build without CXX11_ABI!")
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
ENDIF()

IF(WITH_DEBUG)
  MESSAGE(STATUS "Build with debug mode!")
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O0 -fno-omit-frame-pointer -fPIC -fno-strict-aliasing ${EXTRA_SAN_FLAG}")
ELSE()
  MESSAGE(STATUS "Build with release mode!")
  SET(CMAKE_BUILD_TYPE "RelWithDebInfo")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O3 -fPIC -fno-strict-aliasing ")
ENDIF()

find_package(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})
LINK_DIRECTORIES(${Protobuf_LIBRARIES})

INCLUDE_DIRECTORIES (
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_BINARY_DIR}/include/aliconsensus
  ${CMAKE_CURRENT_BINARY_DIR}/include/easy
  ${CMAKE_CURRENT_SOURCE_DIR}/../../
  ${CMAKE_CURRENT_SOURCE_DIR}/../../../../include
  )

ADD_DEFINITIONS(-w)
ADD_LIBRARY (polar_consensus_wrapper SHARED ${AC_SRC})
TARGET_LINK_LIBRARIES (polar_consensus_wrapper protobuf ${EXTRA_LINK})

TARGET_LINK_LIBRARIES (polar_consensus_wrapper
  "-Wl,--whole-archive"
  ${CMAKE_CURRENT_BINARY_DIR}/lib/libmyeasy.a
  ${CMAKE_CURRENT_BINARY_DIR}/lib/libaliconsensus.a
  "-Wl,--no-whole-archive" ${EXTRA_LINK})

INSTALL(TARGETS polar_consensus_wrapper DESTINATION lib)
INSTALL(FILES ${INC_FILE} DESTINATION include)
INSTALL(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include/easy DESTINATION include)
