/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oulib.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sun.media.jai.codec.ImageCodec;
import com.sun.media.jai.codec.ImageDecoder;
import com.sun.media.jai.codec.TIFFDecodeParam;
import com.sun.media.jai.codec.TIFFEncodeParam;

import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;

import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.ImageWriteException;
import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;
import org.apache.commons.imaging.formats.tiff.TiffImageMetadata;
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfo;
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfoAscii;
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfoByte;
import org.apache.commons.imaging.formats.tiff.write.TiffImageWriterLossy;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputDirectory;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputField;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputSet;
import oulib.aws.exceptions.NoMatchingTagInfoException;

/**
 *
 * @author Tao Zhao
 */
public class S3Util {
    
    /**
     * Check if an Amazon S3 folder exists
     * 
     * @param folderName : folder name to find
     * @param keyMap : the collection of path to check
     * @return : boolean
     */
    public static boolean folderExitsts(String folderName, Map<String, String> keyMap){
        for (String key : keyMap.keySet()) {
            if(null != key && key.contains("/"+folderName+"/")){
                return true;
            }
        } 
        return false;
    }
    
    /**
     * 
     * @param bucketName
     * @param folderName
     * @param client
     * @return : a map of keys with keyset of object keys
     */
    public static Map<String, String> getBucketObjectKeyMap(String bucketName, String folderName, AmazonS3 client){
        final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        Map<String, String> keyMap = new HashMap<>();
            
        do {               
            result = client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {                
                String key = objectSummary.getKey();
                keyMap.put(key, key);
            }            
            req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated() == true ); 
        
        return keyMap;
    }
    
    /**
     * Creates an AWS S3 folder
     * 
     * @param bucketName
     * @param folderName
     * @param client 
     */
    public static void createFolder(String bucketName, String folderName, AmazonS3 client){
    	
    	try{
    	
	    	ObjectMetadata metadata = new ObjectMetadata();
	    	metadata.setContentLength(0);
	
	    	InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
	
	    	PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName+"/", emptyContent, metadata);
	
	    	client.putObject(putObjectRequest);
	    	
	    	System.out.println("Sucessfully created the folder of " + folderName + " in the bucket of " + bucketName);
    	}
    	catch(Exception ex){
    		System.out.println("Failed to create the folder of " + folderName + " in the bucket of " + bucketName);
//    		Logger.getLogger(AwsS3Processor.class.getName÷÷()).log(Level.SEVERE, null, ex);
    	}
    	
    }
    
    /**
     * 
     * @param s3client : S3 cient
     * @param name : bucke name
     * @return : S3 bucket
     */
    public static Bucket getS3BucketByName(AmazonS3 s3client, String name){
        Bucket bucket = null;
        for (Iterator<Bucket> it = s3client.listBuckets().iterator(); it.hasNext();) {
            Bucket bu = it.next();
            if(name.equals(bu.getName())){
                return bu;
            }
        }
        return bucket;
    }
    
    /**
     * Generate a small tiff file from large Tiff S3 bucket object <br>
     * Note: the small tiff file will have the same key path as the original one
     * 
     * @param s3client : S3 client
     * @param s3 : S3 object that con
     * @param targetBucketName : the bucket that stores the small tiff file
     * @return : PutObjectResult
     */
    public static PutObjectResult generateSmallTiff(AmazonS3 s3client, S3Object s3, String targetBucketName, double compressionRate){
        
        PutObjectResult result = null;
        ByteArrayOutputStream bos = null;
        ByteArrayOutputStream os = null;
        ByteArrayInputStream is = null;
        S3ObjectInputStream s = null;
        
        try{
            System.setProperty("com.sun.media.jai.disableMediaLib", "true");

            bos = new ByteArrayOutputStream();
            s = s3.getObjectContent();
            TIFFDecodeParam param = new TIFFDecodeParam();
            ImageDecoder dec = ImageCodec.createImageDecoder("TIFF", s, param);
            RenderedImage image = dec.decodeAsRenderedImage();

            RenderingHints qualityHints = new RenderingHints(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);

            RenderedOp resizedImage = JAI.create("SubsampleAverage", image, compressionRate, compressionRate, qualityHints);
            
            TIFFEncodeParam params = new com.sun.media.jai.codec.TIFFEncodeParam();

            resizedImage = JAI.create("encode", resizedImage, bos, "TIFF", params);
            
            BufferedImage imagenew = resizedImage.getSourceImage(0).getAsBufferedImage();
            
            os = new ByteArrayOutputStream();
            ImageIO.write(imagenew, "tif", os);
            is = new ByteArrayInputStream(os.toByteArray());
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(os.toByteArray().length);
            metadata.setContentType("image/tiff");
            metadata.setLastModified(new Date());
            
            result = s3client.putObject(new PutObjectRequest(targetBucketName, s3.getKey(), is, metadata));
        } catch (IOException | AmazonClientException ex) {
            Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
        } finally{
        	try{
        		if(bos != null){
        			bos.close();
        		}
        		if(os != null){
        			os.close();
        		}
                if(is != null){
                	is.close();
                }
                if(s != null){
                	s.close();
                }
        	}
        	catch(IOException ex){
        		Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
        	}
        }
        
        return result;
    }
    
    /**
     * Pull out Tiff metadata from input S3 object and inject into the 
     * content of target S3 Object;<br>
     * Generate the new output S3 object that has the metadata from input object.
     * 
     * @param s3client : S3 client
     * @param obj1 : input object that provides metadata
     * @param obj2 : target object that receives metadata
     * @param targetBucketName : bucket name for output object
     * @param key : key of the output S3 object
     * 
     * @return PutObjectResult
     */
    public static PutObjectResult copyS3ObjectTiffMetadata(AmazonS3 s3client, S3Object obj1, S3Object obj2, String targetBucketName, String key){
    	
    	PutObjectResult result = null;
    	
        BufferedInputStream bufferedInputStrean = null;
    	ByteArrayOutputStream byteArrayOutputStream = null;
        ByteArrayInputStream byteArrayInputStream = null;
    	ByteArrayInputStream bis = null;
    	S3ObjectInputStream content1 = null;
    	S3ObjectInputStream content2 = null;
    	
    	ImageMetadata metadata1, metadata2;
    	TiffImageMetadata tiffMetadata1, tiffMetadata2;
    	TiffOutputSet output1, output2;
    	
        try {
                content1 = obj1.getObjectContent(); 
                content2 = obj2.getObjectContent(); 

                metadata1 = Imaging.getMetadata(content1, obj1.getKey());
                metadata2 = Imaging.getMetadata(content2, obj2.getKey());

                tiffMetadata1 = (TiffImageMetadata)metadata1;
                tiffMetadata2 = (TiffImageMetadata)metadata2;

                output1 = tiffMetadata1.getOutputSet();
                output2 = tiffMetadata2.getOutputSet();

                TiffOutputDirectory rootDir = output2.getOrCreateRootDirectory();
                TiffOutputDirectory exifDir = output2.getOrCreateExifDirectory();
                TiffOutputDirectory gpsDir = output2.getOrCreateGPSDirectory();
                
                if(null != output1.getRootDirectory()){
                    List<TiffOutputField> fs = output1.getRootDirectory().getFields();
                    for(TiffOutputField f1 : fs){
                        if(null == rootDir.findField(f1.tag)
                                // CANNOT create the output image with this tag included!
                                && !"PlanarConfiguration".equals(f1.tagInfo.name)){
                            rootDir.add(f1);
                        }
                    }
                }

                if(null != output1.getExifDirectory()){
                    for(TiffOutputField f2 : output1.getExifDirectory().getFields()){
                        exifDir.removeField(f2.tagInfo);
                        exifDir.add(f2);
                    }
                }

                if(null != output1.getGPSDirectory()){
                    for(TiffOutputField f3 : output1.getGPSDirectory().getFields()){
                        gpsDir.removeField(f3.tagInfo);
                        gpsDir.add(f3);
                    }
                }
                
                byteArrayOutputStream = new ByteArrayOutputStream();
                TiffImageWriterLossy writerLossy = new TiffImageWriterLossy(output2.byteOrder);
                writerLossy.write(byteArrayOutputStream, output2);
                
                byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(byteArrayOutputStream.toByteArray().length);
                metadata.setContentType("image/tiff");
                metadata.setLastModified(new Date());

                result = s3client.putObject(new PutObjectRequest(targetBucketName, key, byteArrayInputStream, metadata));
			
            } catch (ImageReadException | IOException | ImageWriteException ex) {
                    Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
            } finally{
                    try{
                            if(null != content1){
                                    content1.close();
                            }
                            if(null != content2){
                                    content2.close();
                            }
                            if(null != bufferedInputStrean){
                                    bufferedInputStrean.close();
                            }
                            if(null != byteArrayInputStream){
                                byteArrayInputStream.close();
                            }
                            if(null != byteArrayOutputStream){
                                byteArrayOutputStream.close();
                            }
                            if(null != bis){
                                    bis.close();
                            }
                    } catch(IOException ex){
                            Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
                    }			
            }
            return result;
    }
    
    /**
     *  Get exif technical metadata from S3 object
     * 
     * @param s3client
     * @param s3
     * @return : TiffImageMetadata
     */
    public static TiffImageMetadata retrieveExifMetadata(AmazonS3 s3client, S3Object s3){
        TiffImageMetadata tiffMetadata = null;
        try {
            S3ObjectInputStream is = s3.getObjectContent();
            final ImageMetadata metadata = Imaging.getMetadata(is, s3.getKey());
            tiffMetadata = (TiffImageMetadata)metadata;
        } catch (ImageReadException | IOException ex) {
            Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tiffMetadata;
    }
    
    /**
     * Add data into tiff exif metadata
     * 
     * @param tiffMetadata : TiffImageMetadata
     * @param data : map of data
     */
    public static void addTiffOutputFieldIntoTiffMetadata(TiffImageMetadata tiffMetadata, Map<TagInfo, Object> data){
        try {
            TiffOutputSet output = tiffMetadata.getOutputSet();
            TiffOutputDirectory exifDirectory = output.getOrCreateExifDirectory();
            for(TagInfo tagInfo : data.keySet()){
                addTiffOutputFieldIntoTiffOutputDirectory(exifDirectory, tagInfo, data.get(tagInfo));
            }
        } catch (ImageWriteException | NoMatchingTagInfoException ex) {
            Logger.getLogger(S3Util.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    /**
     * 
     * @param exifDirectory : TiffOutputDirectory
     * @param tagInfo : TagInfo defined the field type
     * @param value : value of the new field
     * @throws ImageWriteException
     * @throws NoMatchingTagInfoException 
     */
    public static void addTiffOutputFieldIntoTiffOutputDirectory(TiffOutputDirectory exifDirectory, TagInfo tagInfo, Object value) 
            throws ImageWriteException, NoMatchingTagInfoException{
        
        exifDirectory.removeField(tagInfo);
        
        if(tagInfo instanceof TagInfoAscii){
            exifDirectory.add((TagInfoAscii)tagInfo, String.valueOf(value));
        }
        else if(tagInfo instanceof TagInfoByte){
            exifDirectory.add((TagInfoByte)tagInfo, String.valueOf(value).getBytes());
        }
        /**
         * Implement more taginfo types here....
         */
        else{
            throw new NoMatchingTagInfoException("Cannot find the matching Exif Tag type information!");
        }
    }
}
