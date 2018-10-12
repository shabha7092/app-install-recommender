package com.shabha.app.grid.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.jpmml.sparkml.ConverterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;
import scala.Tuple2;


public class LRModelGeneratorAvro {

    private static final Logger LOG = LoggerFactory.getLogger(LRModelGeneratorAvro.class);

    private static final String SCHEMA_STRING = "probability label";
    private static final String AVRO_FORMAT = "com.databricks.spark.avro";
    private static final String PART_FILE = "part-m-0000";
    private static final String METRIC_FILE = "metrics.json";

    private SparkContext sparkContext;
    private SQLContext sqlContext;
    private ModelProperties modelProperties;
    private FileSystem fs;
    

    public LRModelGeneratorAvro(Properties properties, SparkConf conf) throws Exception {
        initializeContext(conf); 
        initializeModelProperties(properties);   
    }

    public void initializeContext(SparkConf conf) throws Exception {
        if (conf == null) {
            throw new Exception("Spark Configuration cannot be null");
        }
        try {
            this.sparkContext = new SparkContext(conf);
            this.sqlContext = new SQLContext(sparkContext);  
            this.fs = FileSystem.get(sparkContext.hadoopConfiguration());
        } catch (Exception e) {
            LOG.error("Exception in Initializing Context" + e.getMessage());
            throw new Exception(e);      
        }
        return;
    }

    public void initializeModelProperties(Properties properties) throws Exception {
        if (properties == null || properties.size() == 0) {
            throw new Exception("Properties cannot be null or empty");
        }
        try {
            Gson gson = new Gson();
            Type type = new TypeToken<ModelProperties>() { }.getType();
            if (properties.getProperty(Parameter.TEST_FLAG.value()).equals("true")) {
                File directory = new File(properties.getProperty(Parameter.CONFIG_PATH.value()));
                File[] files = directory.listFiles();
                for (File file : files) {
                    if (file.getName().equals("model_config.json")) {
                        FileReader inputReader = new FileReader(file);
                        BufferedReader bufferReader = new BufferedReader(inputReader);
                        this.modelProperties = gson.fromJson(bufferReader, type); 
                        bufferReader.close();
                        inputReader.close();               
                    }
                }
                
            } else {
                Path path = new Path(properties.getProperty(Parameter.CONFIG_PATH.value()));   
                FileStatus[] files = null;
                files = fs.listStatus(path);
                for (FileStatus file : files) {
                    if (file.getPath().getName().equals("model_config.json")) {
                        FSDataInputStream inputStream = fs.open(file.getPath());
                        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
                        this.modelProperties = gson.fromJson(bufferReader, type);
                        bufferReader.close();
                        inputStream.close();       
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in Initializing Properties" + e.getMessage());
            throw new Exception(e);
        }
        return;
    }

    public CrossValidator buildModel() throws Exception {
        try {
            VectorAssembler assembler = new VectorAssembler();
            assembler.setInputCols(modelProperties.getFeatures()).setOutputCol("features");
            LogisticRegression lr = new LogisticRegression();
            if (modelProperties.getConfiguration().isFit()) {
                lr.fitIntercept();
            }
            lr.setLabelCol(modelProperties.getLabel());
            lr.setFeaturesCol("features");
            Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {assembler, lr});
            ParamMap[] paramGrid = new ParamGridBuilder()
                    .addGrid(lr.maxIter(), modelProperties.getConfiguration().getIterations())
                    .addGrid(lr.regParam(), modelProperties.getConfiguration().getParameters())
                    .addGrid(lr.elasticNetParam(), modelProperties.getConfiguration().getElasticParameters())
                    .build();
            CrossValidator validator = new CrossValidator()
                    .setEstimator(pipeline)
                    .setEvaluator(new BinaryClassificationEvaluator())
                    .setEstimatorParamMaps(paramGrid)
                    .setNumFolds(modelProperties.getConfiguration().getFolds());
            return validator;
        } catch (Exception  e) {
            LOG.error("Exception while build model" + e.getMessage());
            throw new Exception(e);
        }
    }

    public void runModel(CrossValidator validator, Properties properties) throws Exception {
        try {  
            String[] paths = properties.getProperty(Parameter.INPUT_PATH.value()).split(",");           
            Dataset<Row> dataFrame = sqlContext.read().format(AVRO_FORMAT).load(paths);
            List<Row> advertiserRowList = dataFrame.select("advertiser_acct_id").distinct().collectAsList();            
            for (Row advertiserRow : advertiserRowList) {
                String advertiserAcctId = String.valueOf(advertiserRow.get(0));
                Dataset<Row> advertiserDataFrame = dataFrame.where(dataFrame.col("advertiser_acct_id").$eq$eq$eq(advertiserRow.get(0)));
                Dataset<Row>[] splits = advertiserDataFrame.randomSplit(new double[] {0.8, 0.2}, 12345);
                Dataset<Row> trainingDataFrame = splits[0];
                Dataset<Row> testingDataFrame = splits[1];
                Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {validator});
                PipelineModel model = pipeline.fit(trainingDataFrame); 
                Dataset<Row> predictionAndLabels = model.transform(testingDataFrame);  
                PMML pmml = ConverterUtil.toPMML(trainingDataFrame.schema(), model); 
                writeModel(pmml, properties, advertiserAcctId);
                writeMetrics(predictionAndLabels, properties, advertiserAcctId);     
            }
        } catch (Exception e) {
            LOG.error("Exception while executing model" + e.getMessage());
            throw new Exception(e);
        }
    }

    public void writeModel(PMML pmml, Properties properties, String advertiserAcctId) throws Exception { 
        if (pmml == null || properties == null || advertiserAcctId == null) {
            throw new Exception("Pmml or Properties or advertiser_acct_id cannot be null");
        }
        String outputPath = properties.getProperty(Parameter.OUTPUT_PATH.value()) + "/" + advertiserAcctId + "/" + PART_FILE;
        try {
            if (properties.getProperty(Parameter.TEST_FLAG.value()).equals("true")) {  
                File output = new File(outputPath);
                output.getParentFile().mkdirs();
                output.createNewFile();
                FileOutputStream os = new FileOutputStream(output);
                JAXBUtil.marshalPMML(pmml, new StreamResult(os));
                os.close();
            } else {
                Path output = new Path(outputPath);
                FSDataOutputStream os = fs.create(output);
                JAXBUtil.marshalPMML(pmml, new StreamResult(os)); 
                os.close();
            }
            return ;
        } catch (Exception e) {
            LOG.error("Exception while writing model" + e.getMessage());
            throw new Exception(e);
        }     
    }

    public void writeMetrics(Dataset<Row> predictionAndLabels, Properties properties, String advertiserAcctId) throws Exception {
        if (predictionAndLabels == null || properties == null || advertiserAcctId == null) {
            throw new Exception("Predictions or Properties or advertiser_acct_id cannot be null");
        }
        try { 
            Dataset<Row> probablitiesAndLabels = predictionAndLabels.select("probability", "label");
            JavaRDD<Row> probablitiesAndLabelsRDD = probablitiesAndLabels.javaRDD();         
            JavaRDD<Row> scoreAndLabelsRDD = probablitiesAndLabelsRDD.map(row -> RowFactory.create(((double) ((Vector) row.getAs("probability")).toArray()[1]) , (double) row.getAs("label")));
            Dataset<Row> scoreAndLabels = sqlContext.createDataFrame(scoreAndLabelsRDD, getMetricSchema());
            BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels);
            ModelMetrics  modelMetrics = buildModelMetrics(metrics);

            long count = predictionAndLabels.count();
            long predictions = predictionAndLabels.where("prediction == label").count();
            if (predictions != 0L) {
                double accuracy = (double) predictions / count;
                modelMetrics.setAccuracy(accuracy);
            }
            String metricPath = properties.getProperty(Parameter.OUTPUT_PATH.value()) + "/" + advertiserAcctId + "/" + METRIC_FILE;
            Gson gson = new Gson();
            Type type = new TypeToken<ModelMetrics>() { }.getType();

            if (properties.getProperty(Parameter.TEST_FLAG.value()).equals("true")) {  
                File output = new File(metricPath);
                output.getParentFile().mkdirs();
                if (output.exists()) {
                    output.delete();
                }
                output.createNewFile();
                FileOutputStream os = new FileOutputStream(output);
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(os, "UTF-8"));
                gson.toJson(modelMetrics, type, writer);
                writer.close();
                os.close();
            } else {
                Path output = new Path(metricPath);
                FSDataOutputStream os = fs.create(output);
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(os, "UTF-8"));
                gson.toJson(modelMetrics, type, writer);
                writer.close();
                os.close();
            }
        } catch (Exception e) {
            LOG.error("Exception while writing model metrics" + e.getMessage());
            throw new Exception(e);
        }    
    }

    public StructType getMetricSchema() throws Exception {
        try {
            List<StructField> fields = new ArrayList<StructField>();
            for (String fieldName : SCHEMA_STRING.split(" ")) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
                fields.add(field);
            }
            return DataTypes.createStructType(fields);
        } catch (Exception e) {
            LOG.error("Exception while  fetching Metric Schema" + e.getMessage());
            throw new Exception(e);
        }

    }

    public ModelMetrics buildModelMetrics(BinaryClassificationMetrics metrics) throws Exception {
        if (metrics == null) {
            throw new Exception("BinaryClassificationMetrics cannot be null");
        }
        try {
            ModelMetrics modelMetrics = new ModelMetrics();

            modelMetrics.setProduct(modelProperties.getProduct());
            modelMetrics.setVersion(modelProperties.getVersion());
            modelMetrics.setName(modelProperties.getName());
            modelMetrics.setAreaUnderROC(metrics.areaUnderROC());
            modelMetrics.setAreaUnderPRC(metrics.areaUnderPR());

            List<Double> thresholds = metrics.precisionByThreshold()
                    .toJavaRDD().map(t -> new Double(t._1().toString()))
                    .collect();
            modelMetrics.setThresholds(thresholds);
            
            List<Tuple2<Double, Double>> precision = metrics.precisionByThreshold()
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setPrecision(precision);

            List<Tuple2<Double, Double>> recall = metrics.recallByThreshold()
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setRecall(recall);

            List<Tuple2<Double, Double>> f1Score = metrics.fMeasureByThreshold()
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setF1Score(f1Score);

            List<Tuple2<Double, Double>> f2Score = metrics.fMeasureByThreshold(2.0)
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setF2Score(f2Score);

            List<Tuple2<Double, Double>> prc = metrics.pr()
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setPrc(prc);

            List<Tuple2<Double, Double>> roc = metrics.roc()
                    .toJavaRDD()
                    .map(t -> new Tuple2<Double, Double>(new Double(t._1().toString()), new Double(t._2().toString())))
                    .collect();
            modelMetrics.setRoc(roc);
            return modelMetrics;
        } catch (Exception e) {
            LOG.error("Exception while  building Model Metrics" + e.getMessage());
            throw new Exception(e);
        }
    }

    public void cleanUp() throws Exception {
        try {
            sparkContext.stop();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public static void main(String[] args) throws Exception {
          if (args == null || args.length < 4) {
            throw new Exception("Illegal number of arguments");
        }
        Properties properties = new Properties();
        properties.setProperty(Parameter.INPUT_PATH.value(), args[0]);
        properties.setProperty(Parameter.CONFIG_PATH.value(), args[1]);
        properties.setProperty(Parameter.OUTPUT_PATH.value(), args[2]);
        properties.setProperty(Parameter.TEST_FLAG.value(), args[3]);     
        SparkConf conf = null;
        if (properties.getProperty(Parameter.TEST_FLAG.value()).equals("true")) {
            conf = new SparkConf().setMaster("local").setAppName("Growth Model");
        } else {
            conf = new SparkConf().setAppName("Growth Model");
        } 
        LRModelGeneratorAvro modelGenerator = new LRModelGeneratorAvro(properties, conf);
        CrossValidator validator = modelGenerator.buildModel();
        modelGenerator.runModel(validator, properties);
        modelGenerator.cleanUp();
    }
}
