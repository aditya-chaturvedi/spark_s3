package com.aditya.spark.logs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by achat1 on 4/27/16.
 */
@Component
public class LogAnalyzer {

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private LogAnalyzerRDD logAnalyzerRDD;

    public void processLog(String logFile) {
        JavaRDD<ApacheAccessLog> accessLogs = javaSparkContext.textFile(logFile)
                .map(ApacheAccessLog::parseFromLogLine);

        LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
        logStatistics.printToStandardOut();

        javaSparkContext.stop();
    }
}
