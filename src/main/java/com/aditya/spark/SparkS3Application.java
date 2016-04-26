package com.aditya.spark;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkS3Application implements CommandLineRunner{

    @Autowired
    private LogAnalyzerBatchImport logAnalyzerBatchImport;

    @Value("${aws.s3.url}")
    private String logFileUrl;

	public static void main(String[] args) {
		SpringApplication.run(SparkS3Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

        logAnalyzerBatchImport.analyze(logFileUrl);
	}
}
