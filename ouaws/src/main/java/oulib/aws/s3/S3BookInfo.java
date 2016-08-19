/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oulib.aws.s3;

/**
 *
 * @author Tao Zhao
 */
public class S3BookInfo {
    private String bookName;
    private String bucketSourceName;
    private String bucketTargetName;
    private long compressionSize;

    public String getBookName() {
            return bookName;
    }
    public void setBookName(String bookName) {
            this.bookName = bookName;
    }
    public String getBucketSourceName() {
            return bucketSourceName;
    }
    public void setBucketSourceName(String bucketSourceName) {
            this.bucketSourceName = bucketSourceName;
    }
    public String getBucketTargetName() {
            return bucketTargetName;
    }
    public void setBucketTargetName(String bucketTargetName) {
            this.bucketTargetName = bucketTargetName;
    }

    public long getCompressionSize() {
        return compressionSize;
    }

    public void setCompressionSize(long compressionSize) {
        this.compressionSize = compressionSize;
    }
}
