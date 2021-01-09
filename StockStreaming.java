import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import java.net.UnknownHostException;
import java.util.*;

public class StockStreaming {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .appName("StockStreaming")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(jsc, Durations.seconds(10));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.43.218:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("messages");
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
        JavaDStream<String> data = messages.map(v -> {
            return v.value();    // mapping to convert into spark D-Stream
        });
        data.foreachRDD(s->{
            if(!s.isEmpty()){
                Dataset<Row> dataset=spark.read().json(s);
                dataset.write().partitionBy("name").format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").mode(SaveMode.Append).save("hdfs://masternode:9000/data");

            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
