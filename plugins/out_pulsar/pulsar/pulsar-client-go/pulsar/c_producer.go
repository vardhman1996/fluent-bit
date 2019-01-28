//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

/*
#include "c_go_pulsar.h"
*/
import "C"
import (
	"context"
	"runtime"
	"time"
	"unsafe"
)

type createProducerCtx struct {
	client   *client
	callback func(producer Producer, err error)
	conf     *C.pulsar_producer_configuration_t
}

//export pulsarCreateProducerCallbackProxy
func pulsarCreateProducerCallbackProxy(res C.pulsar_result, ptr *C.pulsar_producer_t, ctx unsafe.Pointer) {
	producerCtx := restorePointer(ctx).(createProducerCtx)

	C.pulsar_producer_configuration_free(producerCtx.conf)

	if res != C.pulsar_result_Ok {
		producerCtx.callback(nil, newError(res, "Failed to create Producer"))
	} else {
		p := &producer{client: producerCtx.client, ptr: ptr}
		runtime.SetFinalizer(p, producerFinalizer)
		producerCtx.callback(p, nil)
	}
}

func createProducerAsync(client *client, options ProducerOptions, callback func(producer Producer, err error)) {
	if options.Topic == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "topic is required when creating producer"))
		return
	}

	conf := C.pulsar_producer_configuration_create()

	if options.Name != "" {
		cName := C.CString(options.Name)
		defer C.free(unsafe.Pointer(cName))
		C.pulsar_producer_configuration_set_producer_name(conf, cName)
	}

	// If SendTimeout is 0, we'll leave the default configured value on C library
	if options.SendTimeout > 0 {
		timeoutMillis := options.SendTimeout.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_producer_configuration_set_send_timeout(conf, C.int(timeoutMillis))
	} else if options.SendTimeout < 0 {
		// Set infinite publish timeout, which is specified as 0 in C API
		C.pulsar_producer_configuration_set_send_timeout(conf, C.int(0))
	}

	if options.MaxPendingMessages != 0 {
		C.pulsar_producer_configuration_set_max_pending_messages(conf, C.int(options.MaxPendingMessages))
	}

	if options.MaxPendingMessagesAcrossPartitions != 0 {
		C.pulsar_producer_configuration_set_max_pending_messages_across_partitions(conf, C.int(options.MaxPendingMessagesAcrossPartitions))
	}

	if options.BlockIfQueueFull {
		C.pulsar_producer_configuration_set_block_if_queue_full(conf, cBool(options.BlockIfQueueFull))
	}

	switch options.MessageRoutingMode {
	case RoundRobinDistribution:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_RoundRobinDistribution)
	case UseSinglePartition:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_UseSinglePartition)
	case CustomPartition:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_CustomPartition)
	}

	switch options.HashingScheme {
	case JavaStringHash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_JavaStringHash)
	case Murmur3_32Hash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_Murmur3_32Hash)
	case BoostHash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_BoostHash)
	}

	if options.CompressionType != NoCompression {
		C.pulsar_producer_configuration_set_compression_type(conf, C.pulsar_compression_type(options.CompressionType))
	}

	if options.MessageRouter != nil {
		C._pulsar_producer_configuration_set_message_router(conf, savePointer(&options.MessageRouter))
	}

	if options.Batching {
		C.pulsar_producer_configuration_set_batching_enabled(conf, cBool(options.Batching))
	}

	if options.BatchingMaxPublishDelay != 0 {
		delayMillis := options.BatchingMaxPublishDelay.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_producer_configuration_set_batching_max_publish_delay_ms(conf, C.ulong(delayMillis))
	}

	if options.BatchingMaxMessages != 0 {
		C.pulsar_producer_configuration_set_batching_max_messages(conf, C.uint(options.BatchingMaxMessages))
	}

	if options.Properties != nil {
		for key, value := range options.Properties {
			cKey := C.CString(key)
			cValue := C.CString(value)

			C.pulsar_producer_configuration_set_property(conf, cKey, cValue)

			C.free(unsafe.Pointer(cKey))
			C.free(unsafe.Pointer(cValue))
		}
	}

	topicName := C.CString(options.Topic)
	defer C.free(unsafe.Pointer(topicName))

	C._pulsar_client_create_producer_async(client.ptr, topicName, conf,
		savePointer(createProducerCtx{client,callback, conf}))
}

type topicMetadata struct {
	numPartitions int
}

func (tm *topicMetadata) NumPartitions() int {
	return tm.numPartitions
}

//export pulsarRouterCallbackProxy
func pulsarRouterCallbackProxy(msg *C.pulsar_message_t, metadata *C.pulsar_topic_metadata_t, ctx unsafe.Pointer) C.int {
	router := restorePointerNoDelete(ctx).(*func(msg Message, metadata TopicMetadata) int)
	partitionIdx := (*router)(&message{msg}, &topicMetadata{int(C.pulsar_topic_metadata_get_num_partitions(metadata))})
	return C.int(partitionIdx)
}

/// Producer

type producer struct {
	client *client
	ptr    *C.pulsar_producer_t
}

func producerFinalizer(p *producer) {
	C.pulsar_producer_free(p.ptr)
}

func (p *producer) Topic() string {
	return C.GoString(C.pulsar_producer_get_topic(p.ptr))
}

func (p *producer) Name() string {
	return C.GoString(C.pulsar_producer_get_producer_name(p.ptr))
}

func (p *producer) Send(ctx context.Context, msg ProducerMessage) error {
	c := make(chan error)
	p.SendAsync(ctx, msg, func(msg ProducerMessage, err error) { c <- err; close(c) })

	select {
	case <-ctx.Done():
		return ctx.Err()

	case cm := <-c:
		return cm
	}
}

type sendCallback struct {
	message ProducerMessage
	callback func(ProducerMessage, error)
}

//export pulsarProducerSendCallbackProxy
func pulsarProducerSendCallbackProxy(res C.pulsar_result, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	sendCallback := restorePointer(ctx).(sendCallback)

	if res != C.pulsar_result_Ok {
		sendCallback.callback(sendCallback.message, newError(res, "Failed to send message"))
	} else {
		sendCallback.callback(sendCallback.message, nil)
	}
}

func (p *producer) SendAsync(ctx context.Context, msg ProducerMessage, callback func(ProducerMessage, error)) {
	cMsg := buildMessage(msg)
	defer C.pulsar_message_free(cMsg)

	C._pulsar_producer_send_async(p.ptr, cMsg, savePointer(sendCallback{msg, callback}))
}

func (p *producer) Close() error {
	c := make(chan error)
	p.CloseAsync(func(err error) { c <- err; close(c) })
	return <-c
}

func (p *producer) CloseAsync(callback func(error)) {
	C._pulsar_producer_close_async(p.ptr, savePointer(callback))
}

//export pulsarProducerCloseCallbackProxy
func pulsarProducerCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(error))

	if res != C.pulsar_result_Ok {
		callback(newError(res, "Failed to close Producer"))
	} else {
		callback(nil)
	}
}
