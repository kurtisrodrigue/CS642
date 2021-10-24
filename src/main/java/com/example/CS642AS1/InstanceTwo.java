package com.example.CS642AS1;

import java.io.IOException;
import java.util.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.Writer;
import java.lang.System.*;
import java.text.SimpleDateFormat;



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

public class InstanceTwo {

    public static void main(String[] args) throws IOException {

        /*
         *
         *  INSTANCE NUMBER 1:
         *
         */

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

        ArrayList<String> images = new ArrayList<>();
        Map<String, List<TextDetection>> dict = new HashMap<String, List<TextDetection>>();
        boolean end_flag = false;
        while(!end_flag)
        {
            List<Message> messages = receiveMessages(sqsClient, queueUrl);
            System.out.println("messages size: " + messages.size());
            if(messages.size() > 0)
            {
                for (Message message : messages)
                {
                    System.out.println("message body: " + message.body());

                    String body = message.body();
                    if(body.compareTo("-1") == 0)
                    {
                        end_flag = true;
                        break;
                    }
                    images.add(message.body());
                    List<TextDetection> detections = detectTextLabels(rekClient, bucket, message.body());
                    dict.put(message.body(), detections);
                }
                deleteMessages(sqsClient, queueUrl, messages);
            }
        }
        System.out.println("images " + images);

        sqsClient.close();
        rekClient.close();
        s3.close();
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("output.txt"), "utf-8"))) {
            for( String key : dict.keySet())
            {
                writer.write(key);
                List<TextDetection> detections = dict.get(key);
                for( TextDetection detect : detections)
                {
                    writer.write(" " + detect.detectedText() + " " + detect.confidence());
                }
                writer.write(System.lineSeparator());
            }
        }
    }

    public static List<TextDetection> detectTextLabels(RekognitionClient rekClient, String bucket, String image)
    {

        try {

            S3Object s3Object = S3Object.builder()
                    .bucket(bucket)
                    .name(image)
                    .build() ;

            Image myImage = Image.builder()
                    .s3Object(s3Object)
                    .build();

            DetectTextRequest textRequest = DetectTextRequest.builder()
                    .image(myImage)
                    .build();

            DetectTextResponse textResponse = rekClient.detectText(textRequest);
            List<TextDetection> textCollection = textResponse.textDetections();

            return textCollection;

        } catch (RekognitionException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public static  List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {

        System.out.println("\nReceive messages");

        try {
            // snippet-start:[sqs.java2.sqs_example.retrieve_messages]
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
        // snippet-end:[sqs.java2.sqs_example.retrieve_messages]
    }

    public static void deleteMessages(SqsClient sqsClient, String queueUrl,  List<Message> messages) {
        System.out.println("\nDelete Messages");
        // snippet-start:[sqs.java2.sqs_example.delete_message]

        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
            // snippet-end:[sqs.java2.sqs_example.delete_message]

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}