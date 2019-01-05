package test.join.filter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class TrainingBloomfilter {

  public static int getOptimalBloomFilterSize(int numRecords,float falsePosRate) {
      int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
      return size;
  }

  public static int getOptimalK(float numMembers, float vectorSize) {
      return (int) Math.round(vectorSize / numMembers * Math.log(2));
  }

  public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("df.default.name", "hdfs://master:9000");
		
      Path inputFile = new Path("hdfs://master:9000/usr/input/bloom/user1.txt");

      int numMembers = Integer.parseInt("10");

      float falsePosRate = Float.parseFloat("0.01");

      Path bfFile = new Path("hdfs://master:9000/usr/input/bloom/bloom.bin");

      // Calculate our vector size and optimal K value based on approximations

      int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);

      int nbHash = getOptimalK(numMembers, vectorSize);



      // create new Bloom filter

      BloomFilter filter = new BloomFilter(vectorSize, nbHash,Hash.MURMUR_HASH);
      // Open file for read
      System.out.println("Training Bloom filter of size " + vectorSize
              + " with " + nbHash + " hash functions, " + numMembers
              + " approximate number of records, and " + falsePosRate
              + " false positive rate");

      String line = null;

      int numRecords = 0;

      FileSystem fs = inputFile.getFileSystem(conf);

      for (FileStatus status : fs.listStatus(inputFile)) {

          BufferedReader rdr;

          // if file is gzipped, wrap it in a GZIPInputStream

          if (status.getPath().getName().endsWith(".gz")) {

              rdr = new BufferedReader(new InputStreamReader(
                      new GZIPInputStream(fs.open(status.getPath()))));
          } else {
              rdr = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
          }

          System.out.println("Reading " + status.getPath());

          while ((line = rdr.readLine()) != null) {

              filter.add(new Key(line.getBytes()));

              ++numRecords;

          }
          rdr.close();
      }

      System.out.println("Trained Bloom filter with " + numRecords
              + " entries.");
      System.out.println("Serializing Bloom filter to HDFS at " + bfFile);

      FSDataOutputStream strm = fs.create(bfFile);

      filter.write(strm);

      strm.flush();

      strm.close();

      System.out.println("Done training Bloom filter.");



  }
}