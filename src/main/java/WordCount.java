import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<String> stringDataSource = executionEnvironment.fromElements("a", "b b", "c c c");
    stringDataSource.flatMap(new LineSplitter()).groupBy(0).sum(1).printToErr();
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) {
      String[] words = text.split(" ");
      for (String word : words) {
        collector.collect(new Tuple2<>(word, 1));
      }
    }
  }
}
