//
// Created by Vardhman Mehta on 2019-01-11.
//

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <msgpack.h>

#include "pulsar.h"
//#include <pulsar/c/client.h>
#include <pulsar/c/client_configuration.h>


static int cb_pulsar_init(struct flb_output_instance *ins,
                          struct flb_config *config, void *data)
{
    pulsar_client_configuration_t *conf;

    pulsar_client_configuration_free(conf);
//    pulsar_client_t *client = pulsar_client_create("pulsar://localhost:6650", conf);

//    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
//    pulsar_consumer_configuration_set_consumer_type(consumer_conf, pulsar_ConsumerShared);
    return 0;
}

static void cb_pulsar_flush(void *data, size_t bytes,
                            char *tag, int tag_len,
                            struct flb_input_instance *i_ins,
                            void *out_context,
                            struct flb_config *config)
{
    printf("Hello pulsar!");
    FLB_OUTPUT_RETURN(FLB_OK);
}


static int cb_pulsar_exit(void *data, struct flb_config *config)
{
    return 0;
}

struct flb_output_plugin out_pulsar_plugin = {
        .name         = "pulsar",
        .description  = "Plugin to publish messages to pulsar topic",
        .cb_init      = cb_pulsar_init,
        .cb_flush     = cb_pulsar_flush,
        .cb_exit      = cb_pulsar_exit,
        .flags        = 0,
};
