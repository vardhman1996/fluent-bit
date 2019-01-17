

#ifndef FLB_OUT_PULSAR_CONF_H
#define FLB_OUT_PULSAR_CONF_H

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_output.h>

#define FLB_PULSAR_BROKER "pulsar://127.0.0.1:6650"
#define FLB_PULSAR_TOPIC "fluent-bit"

struct flb_pulsar *flb_pulsar_conf_create(struct flb_output_instance *ins,
                                            struct flb_config *config);

#endif