#FLB_OPTION(LINK_STATIC  On)
FLB_OPTION(BUILD_TESTS  OFF)
FLB_OPTION(BUILD_PYTHON_WRAPPER OFF)
add_subdirectory(pulsar/pulsar-client-cpp)

#include_directories(pulsar-client-cpp/include/pulsar/c)
#include_directories(pulsar-client-cpp/lib/c)
include_directories(pulsar/pulsar-client-cpp/include/)

set(src pulsar.c)

FLB_PLUGIN(out_pulsar "${src}" "")
#target_include_directories(flb-plugin-out_pulsar PUBLIC pulsar/pulsar-client-cpp)
#target_link_libraries(flb-plugin-out_pulsar ${COMMON_LIBS})