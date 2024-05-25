#STARTING KAFKA SERVER
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic test_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1



#FOR COMPILING THINGS
g++ -std=c++11 producer.cpp message.pb.cc -lrdkafka -lprotobuf -lpthread -o producer
g++ -std=c++11 consumer.cpp message.pb.cc -lrdkafka -lprotobuf -lpthread -o consumer
protoc --cpp_out=. message.proto



