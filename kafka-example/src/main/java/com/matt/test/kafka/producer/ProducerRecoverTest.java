package com.matt.test.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * @author matt
 * @date 2019-05-25 22:54
 */
public class ProducerRecoverTest {

    private static KafkaProducer initKafkaProducer(String transactionalId, String servers) {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.request.size", Integer.valueOf(4194304));
        props.put("batch.size", Integer.valueOf(4194304));
        props.put("retries", Integer.valueOf(3));
        props.put("linger.ms", Integer.valueOf(50));
        props.put("client.id", "ProducerIdempotenceExample");
        props.put("bootstrap.servers", servers);
        props.put("enable.idempotence", "true");
        props.put("transactional.id", transactionalId);
        props.put("acks", "all");
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    private static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    //note: 获得某个类的值
    private static Object getValue(Object object, String fieldName) {
        return getValue(object, object.getClass(), fieldName);
    }

    private static Object getValue(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    //note: 设置相应的 value
    private static void setValue(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private static Enum<?> getEnum(String enumFullName) {
        String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
        if (x.length == 2) {
            String enumClassName = x[0];
            String enumName = x[1];
            try {
                Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
                return Enum.valueOf(cl, enumName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Incompatible KafkaProducer version", e);
            }
        }
        return null;
    }

    public static void main(String[] args) {
        String transactionalId = args[0];
        String servers = args[1];
        KafkaProducer producer = initKafkaProducer(transactionalId, servers);

        // recover transactionManager
        Object transactionManager = getValue(producer, "transactionManager");
        Object sequenceNumbers = getValue(transactionManager, "sequenceNumbers");

        invoke(transactionManager, "transitionTo",
            getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
        invoke(sequenceNumbers, "clear");

        // recover producerIdAndEpoch
        long producerId = 100l;
        short epoch = 2;
        Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
        setValue(producerIdAndEpoch, "producerId", producerId);
        setValue(producerIdAndEpoch, "epoch", epoch);

        invoke(transactionManager, "transitionTo",
            getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

        invoke(transactionManager, "transitionTo",
            getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));

        setValue(transactionManager, "transactionStarted", true);

        System.out.println("KafkaProducer recover success.");
        producer.commitTransaction();
    }
}
