import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;


public class Main {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.out.println("Brak parametrów wejściowych : 1:inputDir, 2:outputDir");
        } else {
        String inputDir =  args[0];
        String outputDir = args[1];

            System.out.println("Tworzenie konfiguracji");
            Configuration conf = new Configuration();

            System.out.println("Tworzenie joba");
            Job jobConf = new Job(conf, "fingersStdDev");
            jobConf.setJarByClass(Main.class);
            jobConf.setJobName("job");

            FileInputFormat.addInputPath(jobConf, new Path(args[0]));
            FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

            System.out.println("Uruchamianie mappera");
            jobConf.setMapperClass(MapClass.class);
            jobConf.setMapOutputKeyClass(Text.class);
            jobConf.setMapOutputValueClass(DoubleWritable.class);

            System.out.println("Uruchamianie reducera");
            jobConf.setReducerClass(FingerMeasurementReducer.class);
            jobConf.setOutputKeyClass(Text.class);
            jobConf.setOutputValueClass(DoubleWritable.class);

            System.out.println("Czekanie na zakończenie joba");
            jobConf.waitForCompletion(true);
            System.out.println("Koniec joba");
        }
    }
    public static class MapClass extends Mapper<Object, Text, Text, DoubleWritable>{
        public void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            JSONObject json = new JSONObject(value.toString());

            int series = json.getInt("series");
            String hand = json.getString("side");
            Double first  = json.getJSONObject("features2D").getDouble("first");
            Double second = json.getJSONObject("features2D").getDouble("second");
            Double third  = json.getJSONObject("features2D").getDouble("third");
            Double fourth = json.getJSONObject("features2D").getDouble("fourth");
            Double fifth  = json.getJSONObject("features2D").getDouble("fifth");

            context.write(new Text(series + "_" + hand + "_first"), new DoubleWritable(first));
            context.write(new Text(series + "_" + hand + "_second"), new DoubleWritable(second));
            context.write(new Text(series + "_" + hand + "_third"), new DoubleWritable(third));
            context.write(new Text(series + "_" + hand + "_fourth"), new DoubleWritable(fourth));
            context.write(new Text(series + "_" + hand + "_fifth"), new DoubleWritable(fifth));
        }
    }

    public static class FingerMeasurementReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable val = new DoubleWritable();
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws java.io.IOException, InterruptedException {

            double suma      = 0;
            int    liczba    = 0;
            double srednia   = 0;
            double odleglosc = 0;
            List<Double> lista = new ArrayList<Double>();
            for (DoubleWritable value : values) {
                double d = value.get();
                lista.add(d);
                suma += d;
                ++liczba;
            }
            srednia = suma/liczba;
            for (Double d : lista) {
                odleglosc += (d-srednia)*(d-srednia);
            }
            val.set((double)Math.sqrt(odleglosc/liczba));
            context.write(key, val);
        }
    }
}
