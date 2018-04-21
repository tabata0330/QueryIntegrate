import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class queryIntegrateMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // hadoopの設定に関する処理
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Word Count");

        // Map, Reduceに使うクラスの指定
        job.setJarByClass(queryIntegrateMain.class);
        job.setMapperClass(queryIntegrateMapper.class);
        job.setReducerClass(queryIntegrateReducer.class);

        // 出力の型に関する情報
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        // 入出力先の指定
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // jobの実行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new queryIntegrateMain(), args));
    }
}