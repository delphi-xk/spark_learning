import com.google.gson.Gson;
import org.junit.Test;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017/10/23.
 */
public class JavaTest {


    @Test
    public void testDateFormatter(){
        String dateStr = "2017-02-3a";
        TemporalAccessor accessor = DateTimeFormatter.ofPattern("yyyy-MM-dd").parse(dateStr);
        //System.out.println(accessor);

    }

    @Test
    public void testSimpleFormatter() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        String dateStr = "2017-05-2a3b";
        String dateStr2 = "2017-05-40b";
        System.out.println(format.parse(dateStr));
        System.out.println(format.parse(dateStr2));
    }


    @Test
    public void testSplit(){
        String reg = "\\b(status:)(\\w+)\\b";
        String reg1 = "\\s(status:)(\\w+)\\s";
        Pattern pattern = Pattern.compile(reg1);
        String s = "run status:success";
        String s2 = " run status:error";
        String s3 = " run status:error  status:success  run status:info xxstatus:success";
        Matcher matcher = pattern.matcher(s3);
        String[] res = s.split("run status:");
        String[] res2 = s2.split("run status:");
/*        System.out.println(res.length);
        System.out.println(res2.length);
        System.out.println(res[1]);
        System.out.println(res2[1]);*/
        while(matcher.find()){
            System.out.println(matcher.group(2));
        }

    }


    @Test
    public void test4(){
        System.out.println(new File("d:/workspace/test.scala").exists());
    }

    @Test
    public void testJson() throws FileNotFoundException {
        File file = new File("d:/workspace/feature_test.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        List<Feature> featureList;
        Gson gson = new Gson();
        int num = 4;
        featureList = reader.lines()
                .map(line -> gson.fromJson(line, Feature.class))
                .filter(f -> f.getFeatureNum() ==num)
                .collect(Collectors.toList());
        System.out.println(gson.toJson(featureList));

    }


    @Test
    public void testJson2() throws Exception {
        File file = new File("d:/workspace/cj1_decd.tree");
        Gson gson = new Gson();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        List<Node> trees = reader.lines()
                .map(line -> gson.fromJson(line, Node.class))
                .filter(node -> node.getValue().equals("funmirtbclientriskdate"))
                .collect(Collectors.toList());
        System.out.println(gson.toJson(trees));


    }

    @Test
    public void testReduce(){
        List<Integer> list = Arrays.asList(1,3,5,7,9);
        int result = list.stream()
                .map( integer -> integer*2)
                .reduce( (a,b) -> a+b)
                .orElse(0);
        System.out.println(result);

    }



    public static void main(String[] args){

    }

    @Test
    public void test5(){
        System.out.println(checkNotEmpty(null));
        System.out.println(checkNotEmpty(""));
        System.out.println(checkNotEmpty("1"));
    }

    @Test
    public void testCmd() throws IOException {
        String command = "/install/spark-1.5.1/bin/spark-submit --class com.hyzs.BusinessTest  --master yarn-cluster " +
                " --num-executors 4  --executor-cores 2  --executor-memory 3G " +
                " --jars /install/spark-1.5.1/lib/datanucleus-api-jdo-3.2.6.jar," +
                "/install/spark-1.5.1/lib/datanucleus-core-3.2.10.jar,/install/spark-1.5.1/lib/datanucleus-rdbms-3.2.9.jar " +
                "  /install/convertLibSVM-0.2-jar-with-dependencies.jar &> /install/log-10-17.out";
        System.out.println(command);
        Runtime.getRuntime().exec(new String[]{"bash", "-c", command});

    }


    @Test
    public void generateFile() throws Exception{
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("d:/workspace/pred_test")));
        for(int i =0;i<=10000;i++){
            Random random = new Random();
            int id = random.nextInt(1000);
            String val = random.nextDouble()*100+"";
            writer.write(id+","+val+"\n");
        }
        writer.close();
    }

    public static boolean checkNotEmpty(String str){
        return !(str == null || str.isEmpty());
    }

}


class Node{
    String pid;
    String cid;
    String name;
    String value;
    List<Node> children;

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }
}


class Feature{
    int featureNum;
    Data data;
    String explain;

    public int getFeatureNum() {
        return featureNum;
    }

    public void setFeatureNum(int featureNum) {
        this.featureNum = featureNum;
    }

    public String getExplain() {
        return explain;
    }

    public void setExplain(String explain) {
        this.explain = explain;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

}

class Data{
    List<Double> x;
    List<Double> y;

    public List<Double> getX() {
        return x;
    }

    public void setX(List<Double> x) {
        this.x = x;
    }

    public List<Double> getY() {
        return y;
    }

    public void setY(List<Double> y) {
        this.y = y;
    }
}