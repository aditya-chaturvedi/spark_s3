package com.aditya.spark;

import com.aditya.spark.example.WordCount;
import com.aditya.spark.logs.LogAnalyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkS3Application implements CommandLineRunner{

    @Autowired
    private LogAnalyzer logAnalyzer;

    @Autowired
    private WordCount wordCount;

    @Value("${aws.s3.url}")
    private String logFile;

    @Value("${input.threshold}")
    private int threshold;

    public static void main(String[] args) {
        SpringApplication.run(SparkS3Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
        if (threshold > 0) {
            wordCount.count(logFile, threshold);
        } else {
            logAnalyzer.processLog(logFile);
        }
    }
}
