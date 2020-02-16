import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DataGenerator {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("YYYYMMddHHmmss");
    //DateTimeFormat.forPattern("YYYYMMddHHmmss").withZoneUTC();


    public void generateDataForRedis(String redisHost, int redisPort) {

        Set<String> impKeys = new HashSet<>();
        Set<String> clicksKeys = new HashSet<>();
        Set<String> notificationKeys = new HashSet<>();
        Set<String> conversionKeys = new HashSet<>();
        Set<String> otherKeys = new HashSet<>();

        for (int z = 0; z <= 50000; z++) {

            String requestId = getRandomRequestId();
            String requestTime = getRandomDate(DATE_TIME_FORMATTER);


            String i = "i" + requestId + requestTime;
            impKeys.add(i);

            if (z % 2 == 0) {
                String c = requestTime + requestId;
                clicksKeys.add(c);
            }

            if (z % 5 == 0) {
                String n = "n" + requestId + requestTime;
                String con = "con" + requestId + requestTime;
                notificationKeys.add(n);
                conversionKeys.add(con);
            }

            otherKeys.add(z + "_" + requestTime);

        }

        try (JedisPool jedisPool = new JedisPool(redisHost, redisPort);
             Jedis jedis = jedisPool.getResource()) {

            impKeys.forEach((i) -> writeKey(jedis, i, "fakePriceI"));
            clicksKeys.forEach((i) -> writeKey(jedis, i, "fakePriceC"));
            notificationKeys.forEach((i) -> writeKey(jedis, i, "fakePriceN"));
            conversionKeys.forEach((i) -> writeKey(jedis, i, "fakeParams"));
            otherKeys.forEach((i) -> writeKey(jedis, i, "some value"));
        }
    }

    private void writeKey(Jedis jedis, String key, String value) {
        System.out.println(key);
        jedis.setnx(key, value);
    }

    private String getRandomRequestId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    private String getRandomDate(DateTimeFormatter formatter) {
        Random r = new Random();
        Instant instant = Instant.now().minusSeconds(r.nextInt(100));
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).format(DATE_TIME_FORMATTER);
    }
}
