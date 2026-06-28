package ma.enset.kafkastreamstext;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsTextApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsTextApplication.class, args);
    }

}
