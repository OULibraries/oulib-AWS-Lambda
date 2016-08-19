/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oulib.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Map;

/**
 *
 * @author Tao Zhao
 */
public class S3TiffProcessor implements RequestHandler<S3BookInfo, String> {
    /**
    * 
    * @param bookInfo : contains the information of the source bucket name, target bucket name, and the name of the book
    * @param context : lambda function runtime context
    * @return :
    * 
    */
    @Override
    public String handleRequest(S3BookInfo bookInfo, Context context) {
		
        AmazonS3 s3client = new AmazonS3Client();	
        Region usEast = Region.getRegion(Regions.US_EAST_1);
        s3client.setRegion(usEast);
        
        try{
            String sourceBucketName = bookInfo.getBucketSourceName();
            String targetBucketName = bookInfo.getBucketTargetName();
            String bookName = bookInfo.getBookName();
            
            // Every book has a folder in the target bucket:
            Map targetBucketKeyMap = S3Util.getBucketObjectKeyMap(targetBucketName, bookName, s3client);
            if(!S3Util.folderExitsts(bookName, targetBucketKeyMap)){
                S3Util.createFolder(targetBucketName, bookName, s3client);
            }
            
            final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(sourceBucketName).withPrefix(bookName + "/data/");
            ListObjectsV2Result result;
            
            do {               
               result = s3client.listObjectsV2(req);
               
               for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                   String key = objectSummary.getKey();
                   if(key.endsWith(".tif") && !targetBucketKeyMap.containsKey(key+".tif")){
                       S3Object object = s3client.getObject(new GetObjectRequest(sourceBucketName, key));
                       System.out.println("Start to generate smaller tif image for the object "+key);
                       S3Util.generateSmallTiffWithTargetSize(s3client, object, targetBucketName, bookInfo.getCompressionSize());
//                       S3Util.copyS3ObjectTiffMetadata(s3client, object, s3client.getObject(new GetObjectRequest(targetBucketName, key)), targetBucketName, key+".tif");
                       System.out.println("Finished to generate smaller tif image for the object "+key+".tif");
//                       break;
                   }
               }
               System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
               req.setContinuationToken(result.getNextContinuationToken());
            } while(result.isTruncated() == true ); 
            
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, \nsuch as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return null;
    }
}
