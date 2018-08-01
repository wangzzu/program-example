package com.matt.test.kafka.producer;

import com.google.gson.JsonObject;

import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka producer example with transaction, produce data which is in json
 */
public class ProducerTransactionExample {

    private static String[] cityType = new String[]{"A", "B", "C", "D", "E", "F", "G"};
    private static final Logger logger = LoggerFactory.getLogger(ProducerTransactionExample.class);


    public static void main(String[] args) {
        Options options = new Options();
        Option input = new Option("b", "bootstrap.servers", true, "the bootstrap.servers producer using");
        input.setRequired(true);
        options.addOption(input);
        Option output = new Option("t", "topic", true, "the topic producer using");
        output.setRequired(true);
        options.addOption(output);
        DefaultParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            String e = cmd.getOptionValue("bootstrap.servers");
            String topic = cmd.getOptionValue("topic");
            Properties props = new Properties();
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("max.request.size", Integer.valueOf(4194304));
            props.put("batch.size", Integer.valueOf(4194304));
            props.put("retries", Integer.valueOf(3));
            props.put("linger.ms", Integer.valueOf(50));
            props.put("client.id", "ProducerIdempotenceExample");
            props.put("bootstrap.servers", e);
            props.put("enable.idempotence", "true");
            props.put("transactional.id", "test-transactional");
            props.put("acks", "all");
            KafkaProducer producer = new KafkaProducer(props);
            int i = 0;
            producer.initTransactions();

            while (true) {
                long timestamp = System.currentTimeMillis();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                try {
                    String e1 = sdf.format(Long.valueOf(timestamp));
                    JsonObject msg = new JsonObject();
                    msg.addProperty("id", Integer.valueOf(i));
                    msg.addProperty("datetime", e1);
                    msg.addProperty("city_rank", cityType[i % 7]);

                    try {
                        producer.beginTransaction();
                        producer.send(new ProducerRecord(topic, "0", msg.toString()));
                        producer.send(new ProducerRecord(topic, "1", msg.toString()));
                        producer.send(new ProducerRecord(topic, "2", msg.toString()));
                        producer.commitTransaction();
                    } catch (ProducerFencedException var19) {
                        var19.printStackTrace();
                        producer.close();
                    } catch (KafkaException var20) {
                        var20.printStackTrace();
                        producer.abortTransaction();
                    }

                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException var18) {
                        var18.printStackTrace();
                    }

                    ++i;
                    if (i % 100 == 0) {
                        logger.info("Producer has sent 100 msgs.");
                    }
                } catch (Exception var21) {
                    var21.printStackTrace();
                    logger.error("Forcing producer close!");
                    producer.flush();
                    producer.close();
                    System.exit(0);
                }
            }
        } catch (ParseException var22) {
            System.out.println(var22.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
    }

}