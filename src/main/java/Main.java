import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import jdk.internal.joptsimple.internal.Strings;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static final String IMPRESSION = "i";
    public static final String CLICK = "c";
    public static final String CONVERSION = "con";
    public static final String NOTIFICATION = "n";

    public static final String DATE_PATTERN = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]";

    private static final String IMPRESSION_PATTERN = "i*" + DATE_PATTERN;
    private static final String CONVERSION_PATTERN = "con*" + DATE_PATTERN;
    private static final String NOTIFICATION_PATTERN = "n*" + DATE_PATTERN;
    //private static final String CLICK_PATTERN = "c[^ampaign]" + DATE_PATTERN + "*";
    private static final String CLICK_PATTERN = DATE_PATTERN + "*";

    private static final String AEROSPIKE_NAMESPACE = "ns";
    private static final String AEROSPIKE_SET = "aerospike-set";

    private static final int cores = Runtime.getRuntime().availableProcessors();

    //Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
    private static final int AEROSPIKE_RECORD_EXPIRATION = (int) TimeUnit.DAYS.toSeconds(90);

    public static final Map<String, String> EVENTS_BY_PATTERN = new TreeMap<>();

    static {
        EVENTS_BY_PATTERN.put(IMPRESSION, IMPRESSION_PATTERN);
        EVENTS_BY_PATTERN.put(CLICK, CLICK_PATTERN);
        EVENTS_BY_PATTERN.put(NOTIFICATION, NOTIFICATION_PATTERN);
        EVENTS_BY_PATTERN.put(CONVERSION, CONVERSION_PATTERN);
    }

    private static String REDIS_HOST = "localhost";
    private static int REDIS_PORT = 6380;

    private static String AEROSPIKE_HOST = "127.0.0.1";
    private static int AEROSPIKE_PORT = 33000;

    private static WritePolicy writePolicy;
    private static DataGenerator dataGenerator = new DataGenerator();

    public static void main(String[] args) {

        try(AerospikeClient aerospikeClient = getAerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);) {

            // Generate test data
            //dataGenerator.generateDataForRedis(REDIS_HOST, REDIS_PORT);

            writePolicy = new WritePolicy();
            writePolicy.sendKey = true;
            writePolicy.expiration = AEROSPIKE_RECORD_EXPIRATION;
            writePolicy.maxRetries = 3;

            // Records processing
            Map<String, Map<String, String>> events = readAllKeysAndValues();
            migrateAllEvents(aerospikeClient, events);

        }

    }

    private static AerospikeClient getAerospikeClient(String host, int port) {
        AerospikeClient client = new AerospikeClient(new ClientPolicy(), new Host(host, port));
        return client;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, String>> readAllKeysAndValues() {
        System.out.println("Start reading");
        long start = System.currentTimeMillis();

        ConcurrentHashMap<String, Map<String, String>> events = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> e : EVENTS_BY_PATTERN.entrySet()) {

            String eventType = e.getKey();
            String pattern = e.getValue();

            updateEventsMap(events, eventType, pattern);

        }

        System.out.println("Reading Time (sec): " + (System.currentTimeMillis() - start) / 1000);
        return events;
    }


    private static void migrateAllEvents(AerospikeClient client, Map<String, Map<String, String>> events) {
        System.out.println("Start migrating");
        long start = System.currentTimeMillis();

        for (Map.Entry<String, Map<String, String>> entry : events.entrySet()) {
            Map<String, String> eventsAndValues = entry.getValue();

            if (!eventsAndValues.isEmpty()) {
                Key key = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, entry.getKey());

                Bin[] bins = getBinsFromEvents(entry.getValue());
                client.put(writePolicy, key, bins);
            }
        }

        System.out.println("All records has been processed. Count: " + events.size());
        System.out.println("base.Migration time (sec): " + (System.currentTimeMillis() - start) / 1000);
    }

    private static Bin[] getBinsFromEvents(Map<String, String> eventsAndValues) {
        List<Bin> bins = new ArrayList<>(eventsAndValues.size());

        for (Map.Entry<String, String> e : eventsAndValues.entrySet()) {

            if (e.getKey().equals(CONVERSION)) {
                // add json-string (params) as value
                bins.add(new Bin(e.getKey(), Value.get(e.getValue())));
            } else {
                bins.add(new Bin(e.getKey(), Value.get(1)));
            }
        }

        return bins.toArray(new Bin[bins.size()]);
    }

    /*
        Process all keys for one event type
     */

    private static void updateEventsMap(ConcurrentHashMap<String, Map<String, String>> events, String eventType, String pattern) {
        AtomicInteger counter = new AtomicInteger();

        try (JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);
             Jedis jedis = jedisPool.getResource()) {

            ScanParams scanParams = new ScanParams();
            scanParams.match(pattern);

            ScanResult<String> scan;
            int cursor = 0;
            do {
                scan = jedis.scan(String.valueOf(cursor), scanParams);
                List<String> result = scan.getResult();

                if (result != null && !result.isEmpty()) {
                    counter.addAndGet(result.size());

                    for (String redisKey : result) {
                        String key = extractKeyForEvent(eventType, redisKey);

                        events.computeIfAbsent(key, value -> new HashMap<>());
                        Map<String, String> eventsForKey = events.get(key);

                        String value = jedis.get(redisKey);
                        if (value != null) {
                            String val = CONVERSION.equals(eventType) ? value : Strings.EMPTY;
                            eventsForKey.put(eventType, val);
                        }

                    }
                }

                cursor = Integer.parseInt(scan.getCursor());
            } while (!scan.isCompleteIteration());

        } catch (Exception e) {
            throw new RuntimeException("Error on reading redis keys", e);
        }

        System.out.println("All records has been readed. Count: " + counter.get() + " " + eventType);
    }

    private static String extractKeyForEvent(String eventType, String redisKey) {
        String key;

        switch (eventType) {
            case CLICK:
                // click has next format: YYYYMMDDhhmmss + requestId
                key =  redisKey.substring(14) + redisKey.substring(0, 14);
                break;
            case CONVERSION:
                key = redisKey.substring(3);
                break;
            default:
                key = redisKey.substring(1);
                break;
        }

        return key;
    }

}