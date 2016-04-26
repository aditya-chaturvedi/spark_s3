package com.aditya.spark;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by achat1 on 4/26/16.
 */
@Component
public class LogAnalyzerBatchImport {

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private SQLContext sqlContext;

    public void analyze(String logFileURL) {

        JavaRDD<ApacheAccessLog> accessLogs = javaSparkContext.textFile(logFileURL)
                .map(ApacheAccessLog::parseFromLogLine);

        LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(sqlContext);
        LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
        logStatistics.printToStandardOut();

        javaSparkContext.stop();
    }
}