package WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.util.Collector;


public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // env.setMaxParallelism(32);
        // 每一步都可以设置并行度

        // 从文件中读取数据
        String inputPath = "/Users/longyan/Documents/ylong/code/java/gitlab/flink_learn_java/src/main/java/WordCount/hello.txt";

        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String,Integer>> resultStream = inputDataStream.flatMap(new WordCountFromFile.MyFlatMapper())
                .keyBy(item->item.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }


}
