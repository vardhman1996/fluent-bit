//
// Created by Vardhman Mehta on 2019-01-11.
//

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <msgpack.h>

#include <stdio.h>
#include <string.h>
#include <pulsar/c/client.h>
#include "pulsar.h"
#include "pulsar_conf.h"


static int cb_pulsar_init(struct flb_output_instance *ins,
                          struct flb_config *config, void *data)
{
    (void) config;
    (void) data;
    struct flb_pulsar *ctx = NULL;
    ctx = flb_pulsar_conf_create(ins, config);
    if (!ctx) {
        flb_error("[out_pulsar] failed to initialize");
        return -1;
    }

    /* Set global context */
    flb_output_set_context(ins, ctx);
    return 0;
}

static void cb_pulsar_flush(void *data, size_t bytes,
                            char *tag, int tag_len,
                            struct flb_input_instance *i_ins,
                            void *out_context,
                            struct flb_config *config)
{
    struct flb_pulsar *ctx = out_context;
    
    const char* my_data = "{'fluent-bit':'asdf'}";

    for (int i = 0; i < 10; i++) {
        pulsar_message_t* message = pulsar_message_create();
        pulsar_message_set_content(message, my_data, strlen(my_data));
        pulsar_result err = pulsar_producer_send(ctx->producer, message);
        if (err == pulsar_result_Ok) {
            flb_info("[out pulsar] message sent successfully");
        } else {
            flb_error("[out pulsar] Failed to publish message: %s\n", pulsar_result_str(err));
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }
        pulsar_message_free(message);
    }

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
