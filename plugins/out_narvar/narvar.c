//
// Created by Vardhman Mehta on 2019-01-10.
//

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <msgpack.h>

#include "narvar.h"

static int cb_narvar_init(struct flb_output_instance *ins,
                                 struct flb_config *config, void *data)
{
    struct flb_out_narvar_config *ctx = NULL;
    (void) ins;
    (void) config;
    (void) data;

    ctx = flb_calloc(1, sizeof(struct flb_out_narvar_config));
    if (!ctx) {
        flb_errno();
        return -1;
    }

    ctx->out_format = FLB_STDOUT_OUT_MSGPACK;
    ctx->json_date_format = FLB_STDOUT_JSON_DATE_DOUBLE;

    ctx->json_date_key = "date";
    ctx->json_date_key_len = strlen(ctx->json_date_key);

    flb_output_set_context(ins, ctx);
    return 0;
}

static void cb_narvar_flush(void *data, size_t bytes,
                                   char *tag, int tag_len,
                                   struct flb_input_instance *i_ins,
                                   void *out_context,
                                   struct flb_config *config)
{
    msgpack_unpacked result;
    size_t off = 0, cnt = 0;
    struct flb_out_narvar_config *ctx = out_context;
    char *buf = NULL;

    (void) i_ins;
    (void) config;
    struct flb_time tmp;
    msgpack_object *p;


    buf = flb_malloc(tag_len + 1);
    if (!buf) {
        flb_errno();
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }
    memcpy(buf, tag, tag_len);

    buf[tag_len] = '\0';
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off)) {
        printf("[%zd] %s: [", cnt++, buf);
        flb_time_pop_from_msgpack(&tmp, &result, &p);
        printf("%"PRIu32".%09lu, ", (uint32_t)tmp.tm.tv_sec, tmp.tm.tv_nsec);
        msgpack_object_print(stdout, *p);
        printf("] my custom plugin proof!!!\n");
    }
    msgpack_unpacked_destroy(&result);
    flb_free(buf);

    FLB_OUTPUT_RETURN(FLB_OK);
}



static int cb_narvar_exit(void *data, struct flb_config *config)
{
    struct flb_out_narvar_config *ctx = data;

    if (!ctx) {
        return 0;
    }

    if (ctx->json_date_key) {
        flb_free(ctx->json_date_key);
    }
    flb_free(ctx);
    return 0;
}


struct flb_output_plugin out_narvar_plugin = {
        .name         = "narvar",
        .description  = "Prints events to STDOUT for Narvar",
        .cb_init      = cb_narvar_init,
        .cb_flush     = cb_narvar_flush,
        .cb_exit      = cb_narvar_exit,
        .flags        = 0,
};