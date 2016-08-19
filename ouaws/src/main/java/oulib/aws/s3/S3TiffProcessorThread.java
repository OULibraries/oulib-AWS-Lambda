/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oulib.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Tao Zhao
 */
public class S3TiffProcessorThread implements Runnable {
	
	public static String OUTPUT_BUCKET_PATH = "/Users/zhao0677/Projects/oulib-AWS-Lambda/ouaws/output/";
    
    private S3BookInfo bookInfo;
    private String filter;
    private AmazonS3 s3client;
    private String output;
    private List<String> tiffList;

    public S3BookInfo getBookInfo() {
        return bookInfo;
    }

    public String getFilter() {
        return filter;
    }

    public AmazonS3 getS3client() {
        return s3client;
    }

    public String getOutput() {
        return output;
    }

    public List<String> getTiffList() {
        return tiffList;
    }

    public void setBookInfo(S3BookInfo bookInfo) {
        this.bookInfo = bookInfo;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setS3client(AmazonS3 s3client) {
        this.s3client = s3client;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void setTiffList(List<String> tiffList) {
        this.tiffList = tiffList;
    }
    
    public S3TiffProcessorThread(AmazonS3 s3client, S3BookInfo bookInfo, String filter, List<String> tiffList){
        this.s3client = s3client;
        this.bookInfo = bookInfo;
        this.filter = filter;
        this.output = "";
        this.tiffList = tiffList;
    }
    
    @Override
    public void run (){
    	
    	String sourceBucketName = bookInfo.getBucketSourceName();
        String targetBucketName = bookInfo.getBucketTargetName();
        String bookName = bookInfo.getBookName();
        
        String bucketFolder = OUTPUT_BUCKET_PATH + File.separator + sourceBucketName;
        File bucketFolderFile = new File(bucketFolder);
        if(!bucketFolderFile.exists()){
        	bucketFolderFile.mkdir();
        }
        
        String bookPath = bucketFolder + File.separator + bookName;
        File bookFile = new File(bookPath);
        if(!bookFile.exists()){
        	bookFile.mkdir();
        }
        
        try{
        
            // Every book has a folder in the target bucket:
            Map targetBucketKeyMap = S3Util.getBucketObjectKeyMap(targetBucketName, bookName, s3client);
            if(!S3Util.folderExitsts(bookName, targetBucketKeyMap)){
                S3Util.createFolder(targetBucketName, bookName, s3client);
            }

//            final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(sourceBucketName).withPrefix(bookName + "/data/");
//            ListObjectsV2Result result;

//            do {               
//                result = s3client.listObjectsV2(req);

                for (String key : tiffList) {
//                    String key = objectSummary.getKey();
                    if(key.contains(".tif") && key.contains(filter) && !targetBucketKeyMap.containsKey(key+".tif")){
                        S3Object object = s3client.getObject(new GetObjectRequest(sourceBucketName, key));
                        output += ("Start to generate smaller tif image for the object "+key);
                        System.out.println("Start to generate smaller tif image for the object "+key);
                        S3Util.generateSmallTiffWithTargetSize(s3client, object, targetBucketName, bookInfo.getCompressionSize());
     //                   S3Util.copyS3ObjectTiffMetadata(s3client, object, s3client.getObject(new GetObjectRequest(targetBucketName, key)), targetBucketName, key+".tif");
                        System.out.println("Finished to generate smaller tif image for the object "+key);
                        output += "Finished to generate smaller tif image for the object "+key;
     //                   break;
                    }
                }
//                output += "Next Continuation Token : " + result.getNextContinuationToken();
                System.out.println(Thread.currentThread().getName() + "'s job is done!");
//                req.setContinuationToken(result.getNextContinuationToken());
//            } while(result.isTruncated() == true ); 
	        
        } catch (AmazonServiceException ase) {
            output += "Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.";
            output += "Error Message:    " + ase.getMessage();
            output += "HTTP Status Code: " + ase.getStatusCode();
            output += "AWS Error Code:   " + ase.getErrorCode();
            output += "Error Type:       " + ase.getErrorType();
            output += "Request ID:       " + ase.getRequestId();
           System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.");
           System.out.println("Error Message:    " + ase.getMessage());
           System.out.println("HTTP Status Code: " + ase.getStatusCode());
           System.out.println("AWS Error Code:   " + ase.getErrorCode());
           System.out.println("Error Type:       " + ase.getErrorType());
           System.out.println("Request ID:       " + ase.getRequestId());
       } catch (AmazonClientException ace) {
           output += "Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, \nsuch as not being able to access the network.";
           output += "Error Message: " + ace.getMessage();
           System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, \nsuch as not being able to access the network.");
           System.out.println("Error Message: " + ace.getMessage());
       } finally{
            outputToFile("/Users/zhao0677/Projects/oulib-AWS-Lambda/ouaws/output/"+sourceBucketName+File.separator+bookName+File.separator+filter+".txt");
        }
    }
    
    public void outputToFile(String filePath){
        try{
            File file = new File(filePath);            
            if(!file.exists()){
                file.createNewFile();                
            }
            try (FileOutputStream op = new FileOutputStream(file)) {
                byte[] loggingIngoBytes = output.getBytes();
                op.write(loggingIngoBytes);
                op.flush();
            }
        }
        catch(FileNotFoundException ex){
            Logger.getLogger(S3TiffProcessorThread.class.getName()).log(Level.SEVERE, "Cannot save output to file.", ex);
        }
        catch(IOException ex){
            Logger.getLogger(S3TiffProcessorThread.class.getName()).log(Level.SEVERE, "Cannot save output to file.", ex);
        }
    }
}
