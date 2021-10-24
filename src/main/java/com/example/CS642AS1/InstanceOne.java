package com.example.CS642AS1;

import java.io.IOException;
import java.util.*;
import java.text.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.Writer;
import java.lang.System.*;


import java.io.FileNotFoundException;
import java.io.InputStream;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.rekognition.model.RekognitionException;
import software.amazon.awssdk.services.rekognition.model.S3Object;
import software.amazon.awssdk.services.rekognition.model.DetectTextRequest;
import software.amazon.awssdk.services.rekognition.model.DetectTextResponse;
import software.amazon.awssdk.services.rekognition.model.TextDetection;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;

//import javax.xml.soap.Text;

public class InstanceOne {

    public static void main(String[] args) throws IOException {

        /*
         *
         *  INSTANCE NUMBER 1:
         *
         */
        String pattern = "MMddyyyyHHmmss";
        DateFormat df = new SimpleDateFormat(pattern);
        Date today = Calendar.getInstance().getTime();
        String todayAsString = df.format(today);

        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder().region(region).build();
        RekognitionClient rekClient = RekognitionClient.builder()
                .region(region)
                .build();

        String queueName = "CS642AS1.fifo";
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        System.out.println(queueUrl);

        String bucket = "unr-cs442";
        String[] keys;
        keys = new String[10];
        for(int i = 1; i <= 10; i++)
        {
            keys[i-1] = i + ".jpeg";
        }
        keys[2] = "3.jpg";
        ArrayList<String> toSend = new ArrayList<String>();

        for(String key : keys)
        {
            System.out.println(key);
            List<Label> labels = getLabelsfromImage(rekClient, bucket, key);
            if(labels != null)
            {
                for (Label label : labels)
                {
                    if (label.name().compareTo("Car") == 0 && label.confidence() > 90)
                    {
                        toSend.add(key);
                        sendMessage(sqsClient, queueUrl, key, todayAsString);
                    }
                }
            }
        }
        sendMessage(sqsClient, queueUrl, "-1", todayAsString);

        System.out.println(toSend);
        System.out.println(queueUrl);
        rekClient.close();
        s3.close();
        sqsClient.close();
    }

    // snippet-start:[rekognition.java2.detect_labels_s3.main]
    public static List<Label> getLabelsfromImage(RekognitionClient rekClient, String bucket, String image)
    {

        try {
            S3Object s3Object = S3Object.builder()
                    .bucket(bucket)
                    .name(image)
                    .build() ;

            Image myImage = Image.builder()
                    .s3Object(s3Object)
                    .build();

            DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
                    .image(myImage)
                    .maxLabels(10)
                    .build();

            DetectLabelsResponse labelsResponse = rekClient.detectLabels(detectLabelsRequest);
            List<Label> labels = labelsResponse.labels();
            System.out.println(bucket + " " + image);

            return labels;

        } catch (RekognitionException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, String message, String messageGroupId) {
        System.out.println("\nSending Message " + message);

        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .messageGroupId(messageGroupId)
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}