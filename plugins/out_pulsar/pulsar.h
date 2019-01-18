//
// Created by Vardhman Mehta on 2019-01-11.
//

#ifndef FLUENT_BIT_PULSAR_H
#define FLUENT_BIT_PULSAR

#include <pulsar/c/client.h>

struct flb_topic_conf {
    char *name;
    char *topic;
    char *broker;
};

struct flb_pulsar {
    /* pulsar configurations */
    struct flb_topic_conf topic_conf;

    /* retrying params */
    int blocked;

    /* Internal configs */
    pulsar_client_configuration_t *conf;
    pulsar_client_t *client;
    pulsar_producer_configuration_t* producer_conf;
    pulsar_producer_t *producer;
};

#endif //FLUENT_BIT_PULSAR_H
