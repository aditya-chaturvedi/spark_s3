package com.aditya.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Created by achat1 on 9/22/15.
 */
@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfig {

    @Value("${fs.s3n.awsAccessKeyId}")
    private String awsAccessKeyId;

    @Value("${fs.s3n.awsSecretAccessKey}")
    private String awsSecretAccessKey;

    @Value("${app.name:spark_s3}")
    private String appName;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext(){
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf());
        javaSparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", awsAccessKeyId);
        javaSparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey);

        return javaSparkContext;
    }

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(javaSparkContext());
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
