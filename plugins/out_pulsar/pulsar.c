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
    int i;
    int ret;
    int array_size = 0;
    int map_size;
    size_t off = 0;
    char *json_buf;
    size_t json_size;
    msgpack_unpacked result;
    msgpack_object root;
    msgpack_object map;
    msgpack_sbuffer tmp_sbuf;
    msgpack_packer tmp_pck;
    msgpack_object *obj;
    struct flb_time tms;

    struct flb_pulsar *ctx = out_context;

    /* Iterate the original buffer and perform adjustments */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off)) {
        array_size++;
    }
    msgpack_unpacked_destroy(&result);
    msgpack_unpacked_init(&result);

    /* Create temporal msgpack buffer */
    msgpack_sbuffer_init(&tmp_sbuf);
    msgpack_packer_init(&tmp_pck, &tmp_sbuf, msgpack_sbuffer_write);
    msgpack_pack_array(&tmp_pck, array_size);

    off = 0;
    while (msgpack_unpack_next(&result, data, bytes, &off)) {
        root = result.data;
        flb_time_pop_from_msgpack(&tms, &result, &obj);
        map = root.via.array.ptr[1];

        map_size = map.via.map.size;
        msgpack_pack_map(&tmp_pck, map_size);

        for (i = 0; i < map_size; i++) {
            msgpack_object *k = &map.via.map.ptr[i].key;
            msgpack_object *v = &map.via.map.ptr[i].val;

            msgpack_pack_object(&tmp_pck, *k);
            msgpack_pack_object(&tmp_pck, *v);
        }
    }

    /* Release msgpack */
    msgpack_unpacked_destroy(&result);

    /* Format to JSON */
    ret = flb_msgpack_raw_to_json_str(tmp_sbuf.data, tmp_sbuf.size,
                                      &json_buf, &json_size);

    msgpack_sbuffer_destroy(&tmp_sbuf);
    if (ret != 0) {
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }

    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, json_buf, strlen(json_buf));
    pulsar_result err = pulsar_producer_send(ctx->producer, message);
    if (err == pulsar_result_Ok) {
        flb_info("[out pulsar] message sent successfully");
    } else {
        flb_error("[out pulsar] Failed to publish message: %s\n", pulsar_result_str(err));
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }
    pulsar_message_free(message);
  
    FLB_OUTPUT_RETURN(FLB_OK);
}


static int cb_pulsar_exit(void *data, struct flb_config *config)
{
    struct flb_pulsar *ctx = data;
    if (!ctx) {
        return 0;
    }
    flb_free(ctx);
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
