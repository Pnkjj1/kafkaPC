#include <iostream>
#include <fstream>
#include <string>
#include <librdkafka/rdkafka.h>
#include "message.pb.h"

void produce_message(const std::string& brokers, const std::string& topic, const MyMessage& msg) {
    // Configure the Kafka producer
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to configure Kafka: " << errstr << std::endl;
        exit(1);
    }

    // Create the Kafka producer handle
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        std::cerr << "Failed to create Kafka producer: " << errstr << std::endl;
        exit(1);
    }

    // Serialize the protobuf message
    std::string serialized_msg;
    if (!msg.SerializeToString(&serialized_msg)) {
        std::cerr << "Failed to serialize message." << std::endl;
        exit(1);
    }

    // Produce the message
    rd_kafka_resp_err_t err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(const_cast<char*>(serialized_msg.data()), serialized_msg.size()),
        RD_KAFKA_V_END
    );

    if (err) {
        std::cerr << "Failed to produce message: " << rd_kafka_err2str(err) << std::endl;
    }

    // Wait for all messages to be delivered
    rd_kafka_flush(rk, 10 * 1000);

    // Clean up
    rd_kafka_destroy(rk);
}

int main() {
    // Initialize protobuf library
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Prepare a protobuf message
    MyMessage msg;
    msg.set_id(123);
    msg.set_content("Hello, Kafka!");

    // Kafka configuration
    std::string brokers = "localhost:9092";
    std::string topic = "test_topic";

    // Produce the message to Kafka
    produce_message(brokers, topic, msg);

    // Shutdown protobuf library
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}

