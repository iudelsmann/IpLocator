import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




public class Main {
  private static File database = new File("/GeoLite2-City.mmdb");
  private static DatabaseReader reader;

  // Regex para extrair IP de uma string aleatoria
  private static final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
  private static final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

  private static final PairFunction<String, String, String> IP_MAPPER =
          new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String x) throws IOException {
              // Tenta extrair um IP
              Matcher matcher = pattern.matcher(x.toString());

              // Caso encontre um IP
              if (matcher.find()) {
                InetAddress ipAddress = InetAddress.getByName(matcher.group());
                CityResponse response = null;
                try {
                  response = reader.city(ipAddress);
                  if(response != null && response.getCity() != null) {
                    City city = response.getCity();
                    String coord = response.getLocation().getLatitude().toString() + "_" + response.getLocation().getLongitude().toString();
                    if(city.getName() != null){
                      return new Tuple2<String, String>(city.getName(), "1" + ";" + coord);
                    }
                  }
                } catch (GeoIp2Exception e) {
                  e.printStackTrace();
                }
              }
              return new Tuple2<String, String>("", "");
            }
          };

  private static final Function2<String, String, String> IP_REDUCER =
          new Function2<String, String, String>() {
            @Override
            public String call(String x, String y){
              String coord = null;
              if(coord == null && x.split(";").length > 1){
                coord = x.split(";")[1];
              }
              Integer sum = Integer.valueOf(x.split(";")[0]) + Integer.valueOf(y.split(";")[0]);;
              return sum.toString() + ";" + coord;
            }
          };

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    reader = new DatabaseReader.Builder(database).build();

    SparkConf conf = new SparkConf().setAppName("org.sparkexample.IpLocator").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile(args[0]);
    JavaPairRDD<String, String> pairs = file.mapToPair(IP_MAPPER);
    JavaPairRDD<String, String> counter = pairs.reduceByKey(IP_REDUCER);

    counter.saveAsTextFile(args[1]);
  }
}