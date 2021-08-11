
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author Conway
 * @date 2021/7/11 14:12
 */
public class SortFlink {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataSet<String> data = env.readTextFile("I:\\KangweiData\\Geolife Trajectories 1.3\\geolife_timestampbig\\geolife.txt");
//        DataSet<Trajactory> dataSet = data.map(line -> {
//            String[] fields = line.split("\t");
//            return new Trajactory( String.valueOf(fields[0].trim()), String.valueOf(fields[1].trim()), String.valueOf(fields[2].trim()), Integer.parseInt(fields[3].trim()), String.valueOf(fields[4].trim()));
//        });

        BatchTableEnvironment table =  BatchTableEnvironment.create(env);

//        Table table1 = table.fromDataSet(dataSet);
//        table1.orderBy("timestamp");
        table.connect(new FileSystem().path("I:\\KangweiData\\chengdu_taxi_processed_data\\compareData\\comparedata2.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("lat", DataTypes.STRING())
                        .field("lot", DataTypes.STRING())
                        .field("timestamp",DataTypes.INT())
                        .field("angle",DataTypes.STRING())
                )
                .createTemporaryTable("Trajectorys1");
        Table traj = table.from("Trajectorys1");
        Table result = traj.orderBy("timestamp");

        table.connect(new FileSystem().path("I:\\KangweiData\\chengdu_taxi_processed_data\\compareData\\comparedata3.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("lat", DataTypes.STRING())
                                .field("lot", DataTypes.STRING())
                                .field("timestamp",DataTypes.INT())
                                .field("angle",DataTypes.STRING())
                )
                .createTemporaryTable("Trajectorys2");
        result.insertInto("Trajectorys2");
//        dataSet.writeAsText("I:\\KangweiData\\Geolife Trajectories 1.3\\geolife_timestampbig\\geolife_flink.txt");
        env.execute();
    }
}
