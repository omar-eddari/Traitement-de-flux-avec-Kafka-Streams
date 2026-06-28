package ma.enset.kafkastreamstext.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TextStreamProcessor {

    @Bean
    public KStream<String, String> process(StreamsBuilder builder) {

        KStream<String, String> input =
                builder.stream("text-input");

        KStream<String, String> cleaned = input.mapValues(value -> {

            if (value == null)
                return "";

            return value
                    .trim()
                    .replaceAll("\\s+", " ")
                    .toUpperCase();
        });
        KStream<String, String> valid =
                cleaned.filter((key, value) -> {

                    if (value.isEmpty())
                        return false;

                    if (value.length() > 100)
                        return false;

                    if (value.contains("HACK"))
                        return false;

                    if (value.contains("SPAM"))
                        return false;

                    if (value.contains("XXX"))
                        return false;

                    return true;
                });

        KStream<String, String> invalid =
                cleaned.filter((key, value) -> {

                    if (value.isEmpty())
                        return true;

                    if (value.length() > 100)
                        return true;

                    if (value.contains("HACK"))
                        return true;

                    if (value.contains("SPAM"))
                        return true;

                    if (value.contains("XXX"))
                        return true;

                    return false;
                });
        valid.to("text-clean");
        invalid.to("text-dead-letter");

        return input;
    }
}