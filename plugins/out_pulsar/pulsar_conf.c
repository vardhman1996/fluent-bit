#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_utils.h>

#include "pulsar.h"
#include "pulsar_conf.h"

struct flb_pulsar *flb_pulsar_conf_create(struct flb_output_instance *ins,
                                            struct flb_config *config)
{
    char *tmp;
    struct flb_pulsar *ctx;
    struct flb_topic_conf *topic_conf;
    
    ctx = flb_calloc(1, sizeof(struct flb_pulsar));
    if (!ctx) {
        flb_error("[out pulsar] failed to initialzie \n");
        return NULL;
    }
    topic_conf = &ctx->topic_conf;

    ctx->conf = pulsar_client_configuration_create();

    /* get broker */
    tmp = flb_output_get_property("broker", ins);
    if (tmp) {
        topic_conf->broker = flb_strdup(tmp);
    } else {
        flb_info("[out pulsar] broker link not provided, using defaut: %s\n", FLB_PULSAR_BROKER); 
        topic_conf->broker = flb_strdup(FLB_PULSAR_BROKER);
    }
    ctx->client = pulsar_client_create(topic_conf->broker, ctx->conf);
    ctx->producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, 1);

    /* get topic name */
    tmp = flb_output_get_property("topic", ins);
    if (tmp) {
        topic_conf->topic = flb_strdup(tmp);
    } else {
        flb_info("[out pulsar] topic name not provided, using default: %s\n", FLB_PULSAR_TOPIC);
        topic_conf->topic = FLB_PULSAR_TOPIC;
    }
    pulsar_result err = pulsar_client_create_producer(ctx->client, topic_conf->topic,
                                            ctx->producer_conf, &ctx->producer);
    if (err != pulsar_result_Ok) {
        flb_error("[out pulsar] Failed to create producer: %s\n", pulsar_result_str(err));
        flb_free(ctx);
        return NULL;
    }

    flb_info("[out pulsar] init success. broker='%s' topic='%s'", topic_conf->broker, topic_conf->topic);
    return ctx;
}