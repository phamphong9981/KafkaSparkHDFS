import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Producer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.2:9092,192.168.43.2:9093");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer
                <String, String>(props);
        String topic = "messages";

        Calendar from = Calendar.getInstance();
        Calendar to = Calendar.getInstance();
        from.add(Calendar.YEAR, -2);

        String[] symbols = new String[] {"INTC", "BABA", "TSLA", "AIR.PA", "GOOG"};
        Map<String, Stock> stocks = YahooFinance.get(symbols);
        Stock google=stocks.get("GOOG");
        Stock intel=stocks.get("INTC");
        Stock alibaba=stocks.get("BABA");
        Stock tesla=stocks.get("TSLA");
        Stock airpay=stocks.get("AIR.PA");
        List<HistoricalQuote> googleHistQuotes = google.getHistory(from, to, Interval.DAILY);
        List<HistoricalQuote> intelHistQuotes = intel.getHistory(from, to, Interval.DAILY);
        List<HistoricalQuote> alibabaHistQuotes = alibaba.getHistory(from, to, Interval.DAILY);
        List<HistoricalQuote> teslaHistQuotes = tesla.getHistory(from, to, Interval.DAILY);
        List<HistoricalQuote> airpayHistQuotes = airpay.getHistory(from, to, Interval.DAILY);
        for (int i = 0; i < 503; i++) {
//            long now=Calendar.getInstance().getTime().getTime();
            Date date = Calendar.getInstance().getTime();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String now = dateFormat.format(date);
            JSONObject gooJSON=new JSONObject();
            gooJSON.put("name","GOOG");
            gooJSON.put("open",googleHistQuotes.get(i).getOpen());
            gooJSON.put("high",googleHistQuotes.get(i).getHigh());
            gooJSON.put("low",googleHistQuotes.get(i).getLow());
            gooJSON.put("close",googleHistQuotes.get(i).getClose());
            gooJSON.put("volume",googleHistQuotes.get(i).getVolume());
            gooJSON.put("time",now);
            JSONObject intelJSON=new JSONObject();
            intelJSON.put("name","INTC");
            intelJSON.put("open",intelHistQuotes.get(i).getOpen());
            intelJSON.put("high",intelHistQuotes.get(i).getHigh());
            intelJSON.put("low",intelHistQuotes.get(i).getLow());
            intelJSON.put("close",intelHistQuotes.get(i).getClose());
            intelJSON.put("volume",intelHistQuotes.get(i).getVolume());
            intelJSON.put("time",now);
            JSONObject alibabaJSON=new JSONObject();
            alibabaJSON.put("name","BABA");
            alibabaJSON.put("open",alibabaHistQuotes.get(i).getOpen());
            alibabaJSON.put("high",alibabaHistQuotes.get(i).getHigh());
            alibabaJSON.put("low",alibabaHistQuotes.get(i).getLow());
            alibabaJSON.put("close",alibabaHistQuotes.get(i).getClose());
            alibabaJSON.put("volume",alibabaHistQuotes.get(i).getVolume());
            alibabaJSON.put("time",now);
            JSONObject teslaJSON=new JSONObject();
            teslaJSON.put("name","TSLA");
            teslaJSON.put("open",teslaHistQuotes.get(i).getOpen());
            teslaJSON.put("high",teslaHistQuotes.get(i).getHigh());
            teslaJSON.put("low",teslaHistQuotes.get(i).getLow());
            teslaJSON.put("close",teslaHistQuotes.get(i).getClose());
            teslaJSON.put("volume",teslaHistQuotes.get(i).getVolume());
            teslaJSON.put("time",now);
            JSONObject airpayJSON=new JSONObject();
            airpayJSON.put("name","AIR.PA");
            airpayJSON.put("open",airpayHistQuotes.get(i).getOpen());
            airpayJSON.put("high",airpayHistQuotes.get(i).getHigh());
            airpayJSON.put("low",airpayHistQuotes.get(i).getLow());
            airpayJSON.put("close",airpayHistQuotes.get(i).getClose());
            airpayJSON.put("volume",airpayHistQuotes.get(i).getVolume());
            airpayJSON.put("time",now);
            producer.send(new ProducerRecord<String, String>(topic, gooJSON.toJSONString()));
            producer.send(new ProducerRecord<String, String>(topic, intelJSON.toJSONString()));
            producer.send(new ProducerRecord<String, String>(topic, alibabaJSON.toJSONString()));
            producer.send(new ProducerRecord<String, String>(topic, teslaJSON.toJSONString()));
            producer.send(new ProducerRecord<String, String>(topic, airpayJSON.toJSONString()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}