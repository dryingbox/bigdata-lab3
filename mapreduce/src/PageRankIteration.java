import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 一次 PageRank 迭代。
 *
 * <p>输入（每行）：{@code 网页 \t 当前PR值 \t 出链1,出链2,...}
 * <p>Mapper：
 * <ul>
 *   <li>把本行网页自己的出链集合以 "#" + 出链 的形式发给本网页（用于在 Reducer 中保留图结构）；</li>
 *   <li>把 PR值/出链数 作为投票发给每一条出链指向的网页。</li>
 * </ul>
 * <p>Reducer：对同一网页收到的所有投票求和，按
 * {@code PR = (1-d)/N + d*(sum + dangling/N)} 计算新 PR 值并保留其出链集合。
 *
 * <p>悬挂节点（无出链的网页）的质量无法在本轮内均分给全图，使用计数器收集后由
 * Driver 传给下一轮；本实验数据集是闭合图（无悬挂节点），因此结果不受这一轮延迟影响。
 */
public class PageRankIteration {

    public static final double DAMPING = 0.85;
    public static final String DANGLE_COUNTER_GROUP = "pagerank";
    public static final String DANGLE_COUNTER = "DANGLING_MASS";     // 以 1e-6 为单位累计
    public static final String DANGLE_CONF = "pagerank.dangling";
    public static final String N_CONF = "pagerank.n";

    /** 保留图结构的标记，避免与数值投票混淆。 */
    public static final String GRAPH_PREFIX = "#";

    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\t");
            if (parts.length < 2) {
                return;
            }
            String page = parts[0];
            double rank;
            try {
                rank = Double.parseDouble(parts[1]);
            } catch (NumberFormatException e) {
                return;
            }
            String links = parts.length > 2 ? parts[2] : "";

            // 1) 自己的出链集合送给本网页
            context.write(new Text(page), new Text(GRAPH_PREFIX + links));

            // 2) 每条出链分得 rank/degree
            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                double share = rank / targets.length;
                for (String target : targets) {
                    String t = target.trim();
                    if (!t.isEmpty()) {
                        context.write(new Text(t), new Text(Double.toString(share)));
                    }
                }
            }
        }
    }

    public static class RankReducer extends Reducer<Text, Text, Text, Text> {
        private int n;
        private double dangling;
        private Counter dangleCounter;

        @Override
        protected void setup(Context context) {
            n = context.getConfiguration().getInt(N_CONF, 1);
            dangling = context.getConfiguration().getDouble(DANGLE_CONF, 0.0);
            dangleCounter = context.getCounter(DANGLE_COUNTER_GROUP, DANGLE_COUNTER);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            String links = "";
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith(GRAPH_PREFIX)) {
                    links = s.substring(GRAPH_PREFIX.length());
                } else {
                    sum += Double.parseDouble(s);
                }
            }
            double rank = (1.0 - DAMPING) / n + DAMPING * (sum + dangling / n);
            if (links.isEmpty()) {
                dangleCounter.increment((long) (rank * 1_000_000L));
            }
            context.write(new Text(key), new Text(Double.toString(rank) + "\t" + links));
        }
    }
}
