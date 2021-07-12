package dev.spider.kafka.command;

import kafka.admin.TopicCommand;

public class TopicCommandAction {


    public static void main(String[] args) {
        creatTopic();
    }

    static void creatTopic() {
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--create",
                "--replication-factor", "1",
                "partitions", "1",
                "--topic", "topic-create-command"
        };
        TopicCommand.main(options);

    }
    //<dependency>
    //            <groupId>org.apache.kafka</groupId>
    //            <artifactId>kafka_2.11</artifactId>
    //            <version>2.0.0</version>
    //        </dependency>
}
