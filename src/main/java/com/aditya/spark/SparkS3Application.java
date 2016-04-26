package com.aditya.spark;

import com.aditya.spark.logs.ApacheAccessLog;
import com.aditya.spark.logs.LogAnalyzerRDD;
import com.aditya.spark.logs.LogStatistics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkS3Application implements CommandLineRunner{

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private LogAnalyzerRDD logAnalyzerRDD;

    @Value("${aws.s3.url}")
    private String logFileUrl;

	public static void main(String[] args) {
		SpringApplication.run(SparkS3Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

        JavaRDD<ApacheAccessLog> accessLogs = javaSparkContext.textFile(logFileUrl)
                .map(ApacheAccessLog::parseFromLogLine);

        LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
        logStatistics.printToStandardOut();

        javaSparkContext.stop();
    }
}
