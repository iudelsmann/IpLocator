import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;

public class IpLocator {

  /**
   * Classe Mapper. Responsavel por extrair strings de IPs dos logs, enviar
   * requisicoes para o servidor localizador, e montar padrao "chave valor" para
   * os reducers.
   *
   */
  public static class IpLocatorMapper extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text concat = new Text();
    private File database = new File("/GeoLite2-City.mmdb");
    private DatabaseReader reader = new DatabaseReader.Builder(database).build();

    // Regex para extrair IP de uma string aleatoria
    private static final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

    public IpLocatorMapper() throws IOException {
    }

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // Tenta extrair um IP
      Matcher matcher = pattern.matcher(value.toString());

      // Caso encontre um IP
      if (matcher.find()) {
        InetAddress ipAddress = InetAddress.getByName(matcher.group());
        CityResponse response = null;
        try {
          response = reader.city(ipAddress);
          if (response != null && response.getCity() != null) {
            City city = response.getCity();
            String coord = response.getLocation().getLatitude().toString() + "_"
                + response.getLocation().getLongitude().toString();
            if (city.getName() != null) {
              word.set(city.getName());
              concat.set("1" + ";" + coord);
              context.write(word, concat);
            }
          }
        } catch (GeoIp2Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Classe Reducer. Responsavel por agrupar entradas com mesma chave (IPs do
   * mesmo local) e somar os valores, assim descobrindo a quantidade de
   * requisicoes de cada origem.
   *
   */
  public static class IpLocatorReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Integer sum = 0;
      String coord = null;
      for (Text val : values) {
        if (coord == null && val.toString().split(";").length > 1) {
          coord = val.toString().split(";")[1];
        }
        sum += Integer.valueOf(val.toString().split(";")[0]);
      }
      result.set(sum.toString() + ";" + coord);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "IpLocator");
    job.setJarByClass(IpLocator.class);
    job.setMapperClass(IpLocatorMapper.class);
    job.setCombinerClass(IpLocatorReducer.class);
    job.setReducerClass(IpLocatorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}