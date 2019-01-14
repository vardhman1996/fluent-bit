FLB_OPTION(LINK_STATIC  On)
FLB_OPTION(BUILD_TESTS  OFF)
add_subdirectory(pulsar-client-cpp EXCLUDE_FROM_ALL)

include_directories(pulsar-client-cpp/include/pulsar/c)
include_directories(pulsar-client-cpp/include/)
include_directories(pulsar-client-cpp/lib/c)

set(src pulsar.c)

FLB_PLUGIN(out_pulsar "${src}" "")
