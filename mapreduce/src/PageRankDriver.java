import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * PageRank 的 MapReduce 驱动：初始化 → 迭代 10 轮 → 输出 (网页,PR值)。
 *
 * <p>运行：
 * <pre>
 *   hadoop jar lab3-pagerank-mr.jar PageRankDriver /lab3/input/links.txt /lab3/state /lab3/output-mr 10
 * </pre>
 * 参数缺省为 links.txt、10 次迭代。
 */
public class PageRankDriver {

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "/lab3/input/links.txt";
        String stateRoot = args.length > 1 ? args[1] : "/lab3/state";
        String output = args.length > 2 ? args[2] : "/lab3/output-mr";
        int iterations = args.length > 3 ? Integer.parseInt(args[3]) : 10;

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", PageRankDriver.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath());
        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(input);
        Path statePath = new Path(stateRoot);
        if (fs.exists(statePath)) {
            fs.delete(statePath, true);
        }

        // ---------------- 0. 读入图并统计网页数 N，生成初始状态（每页 1/N）
        Set<String> pages = new LinkedHashSet<>();
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(inputPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\t", 2);
                String page = parts[0].trim();
                String links = parts.length > 1 ? parts[1].trim() : "";
                pages.add(page);
                if (!links.isEmpty()) {
                    for (String t : links.split(",")) {
                        pages.add(t.trim());
                    }
                }
                rows.add(new String[] {page, links});
            }
        }
        int n = pages.size();
        System.out.println("[PageRank] 输入 " + inputPath + "：网页数 N=" + n);
        conf.setInt(PageRankIteration.N_CONF, n);

        Path initPath = new Path(statePath, "iter0");
        double initial = 1.0 / n;
        try (OutputStream os = fs.create(new Path(initPath, "part-00000"));
             PrintWriter pw = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
            for (String[] row : rows) {
                pw.println(row[0] + "\t" + initial + "\t" + row[1]);
            }
        }

        // ---------------- 1. 迭代 iterations 轮
        double dangling = 0.0;   // 上一轮收集到的悬挂节点质量
        for (int i = 1; i <= iterations; i++) {
            conf.setDouble(PageRankIteration.DANGLE_CONF, dangling);
            Path in = new Path(statePath, "iter" + (i - 1));
            Path out = new Path(statePath, "iter" + i);
            Job job = Job.getInstance(conf, "PageRank-iteration-" + i);
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PageRankIteration.RankMapper.class);
            job.setReducerClass(PageRankIteration.RankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);
            if (!job.waitForCompletion(true)) {
                System.err.println("第 " + i + " 轮迭代失败");
                System.exit(1);
            }
            long micro = job.getCounters()
                    .findCounter(PageRankIteration.DANGLE_COUNTER_GROUP,
                            PageRankIteration.DANGLE_COUNTER).getValue();
            dangling = micro / 1_000_000.0;
            System.out.println("[PageRank] 第 " + i + " 轮完成，悬挂质量=" + dangling);
        }

        // ---------------- 2. 汇总最终结果，按 PR 降序（同值按网页名升序）输出
        List<String[]> result = new ArrayList<>();
        Path finalState = new Path(statePath, "iter" + iterations);
        for (FileStatus st : fs.listStatus(finalState)) {
            if (!st.getPath().getName().startsWith("part-")) {
                continue;
            }
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(st.getPath()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        result.add(new String[] {parts[0], parts[1]});
                    }
                }
            }
        }
        result.sort((a, b) -> {
            int cmp = Double.compare(Double.parseDouble(b[1]), Double.parseDouble(a[1]));
            return cmp != 0 ? cmp : a[0].compareTo(b[0]);
        });

        Path outPath = new Path(output);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        try (OutputStream os = fs.create(new Path(outPath, "pagerank-result.txt"));
             PrintWriter pw = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
            for (String[] r : result) {
                pw.println("(" + r[0] + "," + String.format(java.util.Locale.ROOT, "%.10f",
                        Double.parseDouble(r[1])) + ")");
            }
        }
        System.out.println("[PageRank] 结果已写入 " + new Path(outPath, "pagerank-result.txt")
                + "，共 " + result.size() + " 个网页");
        for (String[] r : result) {
            System.out.println("(" + r[0] + "," + String.format(java.util.Locale.ROOT, "%.10f",
                    Double.parseDouble(r[1])) + ")");
        }
    }
}
