//
// Created by Vardhman Mehta on 2019-01-10.
//

#ifndef SERVER_NARVAR_H
#define SERVER_NARVAR_H

#define FLB_STDOUT_OUT_MSGPACK      0

#define FLB_STDOUT_JSON_DATE_DOUBLE      0

#define FLB_STDOUT_JSON_DATE_ISO8601_FMT "%Y-%m-%dT%H:%M:%S"

struct flb_out_narvar_config {
    int out_format;
    int json_date_format;
    char *json_date_key;

    size_t json_date_key_len;
};

#endif //SERVER_NARVAR_H
