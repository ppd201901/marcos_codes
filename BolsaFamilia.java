import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BolsaFamilia {

  public static class BolsaFamiliaMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String line = value.toString();

      if (line != null && !line.isEmpty() && !line.contains("VALOR PARCELA")){
        String estado = line.split(";")[2].replace("\"", "");
        Double valorRecebidoDouble = Double.parseDouble(line.split(";")[7].replace("\"", "").replace(",", "."));
        int valorRecebido = (int) Math.round(valorRecebidoDouble);
        value.set(estado);
        context.write(value, new IntWritable(valorRecebido));
      }
    }
  }

  public static class BolsaFamiliaReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bolsa familia");
    job.setJarByClass(BolsaFamilia.class);
    job.setMapperClass(BolsaFamiliaMapper.class);
    job.setCombinerClass(BolsaFamiliaReducer.class);
    job.setReducerClass(BolsaFamiliaReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



