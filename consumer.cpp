#include <iostream>
#include <librdkafka/rdkafka.h>
#include "message.pb.h"

void consume_messages(const std::string& brokers, const std::string& group_id, const std::string& topic) {
    // Configure the Kafka consumer
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to configure Kafka: " << errstr << std::endl;
        exit(1);
    }
    if (rd_kafka_conf_set(conf, "group.id", group_id.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to set group ID: " << errstr << std::endl;
        exit(1);
    }

    // Create the Kafka consumer handle
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        std::cerr << "Failed to create Kafka consumer: " << errstr << std::endl;
        exit(1);
    }

    // Configure the consumer to automatically subscribe to the topic
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
    if (err) {
        std::cerr << "Failed to subscribe to topic: " << rd_kafka_err2str(err) << std::endl;
        exit(1);
    }

    // Start consuming messages
    while (true) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                std::cerr << "Reached end of partition: " << rd_kafka_err2str(rkmessage->err) << std::endl;
            } else {
                std::cerr << "Consumer error: " << rd_kafka_err2str(rkmessage->err) << std::endl;
            }
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        // Deserialize the protobuf message
        MyMessage msg;
        if (!msg.ParseFromArray(rkmessage->payload, rkmessage->len)) {
            std::cerr << "Failed to parse protobuf message." << std::endl;
        } else {
            std::cout << "Received message:" << std::endl;
            std::cout << "ID: " << msg.id() << std::endl;
            std::cout << "Content: " << msg.content() << std::endl;
        }

        rd_kafka_message_destroy(rkmessage);
    }

    // Clean up
    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
}

int main() {
    // Initialize protobuf library
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Kafka configuration
    std::string brokers = "localhost:9092";
    std::string group_id = "test_group";
    std::string topic = "test_topic";

    // Consume messages from Kafka
    consume_messages(brokers, group_id, topic);

    // Shutdown protobuf library
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}

