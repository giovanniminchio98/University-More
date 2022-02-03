import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.*;

public class G05HW3 {

    /*
    provided function for process a line of the read input file
     */
    public static Vector strToTuple (String str){
        String[] tokens = str.split(" ");


        double[] data = new double[tokens.length];
        data[0] = Double.parseDouble(tokens[0]);
        data[1] = Double.parseDouble(tokens[1]);

        Vector point = Vectors.dense(data);
        /*Double x = Double.parseDouble(tokens[0]);
        Double y = Double.parseDouble(tokens[1]);
        Tuple2<Double, Double> pair = new Tuple2<>(x,y);*/
        return point;
    }

    public static void main(String[] args)
    {


        //parameters check

        /*if (args.length != 3)
        {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }*/

        // SPARK SETUP

        //spark configuration, give a name to the app
        //SparkConf conf = new SparkConf(true).setAppName("ReviewApp");
        SparkConf conf = new SparkConf(true)
                .setAppName("Homework3")
                .set("spark.locality.wait", "0s");
        //set the sparkConfig=true means that we can give commandos to spark config

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");//"re-call" method that decide the number of warning we want to see during the run

        //POINT 1

        // Read file name
        String file_name = args[0];

        //initial number of clusters
        int kstart = Integer.parseInt(args[1]);

        //number of values of k that the program will test
        int h = Integer.parseInt(args[2]);

        //number of iterations of the Llyod's Alg
        int iter = Integer.parseInt(args[3]);

        //expected size of the sample used to approximate the silhouette  coefficient
        int M = Integer.parseInt(args[4]);

        //number of partitions of the RDD containing the input points and their clustering
        int L = Integer.parseInt(args[5]);

        long start = System.currentTimeMillis();//start counting time
        JavaRDD<Vector> inputPoints = sc.textFile(file_name)
                .map(x -> strToTuple(x)).repartition(L).cache();


        //JavaRDD<String> inputPoints = sc.textFile(file_name).repartition(L).cache();
        long end = System.currentTimeMillis();//start counting time

        System.out.println("time spent to read: " + (end - start) + " ms");
        //POINT 2.1

        /*For every k between kstart and kstart+h-1 does the following*/

        //compute clustering
        long clustering_time=0;

        for(int k=kstart ; k<kstart+h ; k++)
        {
            // Cluster the data into two classes using KMeans
            start = System.currentTimeMillis();
            KMeansModel clusters = KMeans.train(inputPoints.rdd(), k, iter);

            end = System.currentTimeMillis();
            clustering_time=end-start;
            //cluster for each point (just cluster number)
            JavaRDD<Integer> currentClust = clusters.predict(inputPoints);

            //display the clusters (from first to last point of the dataset)
            /*List<Integer> a=new ArrayList<>();
            for(Integer line:currentClust.collect()){
                System.out.println("* "+line);
                a.add(line);
            }*/

            //Broadcast<List<Integer>> cluster_to_points = sc.broadcast(a);

            //now we need to create a javaRDD<Tuple2<Vector, Integer>> fuse together the inputPoint and currentClustering ones
            JavaPairRDD<Vector, Integer> currentClustering = inputPoints.flatMapToPair((document)->{

                Tuple2<Vector, Integer> point = new Tuple2(document,clusters.predict(document));

                ArrayList<Tuple2<Vector, Integer>> list = new ArrayList<>();
                list.add(point);

                return list.iterator();
            });

            int conta_righe=1;

          /*  for(Tuple2<Vector, Integer> line:currentClustering.collect()){
                System.out.println(conta_righe +" "+line._1 + " -- " + line._2);
                conta_righe++;
            }*/

            //POINT 2.2
            //approximate silhuette coeff
            int t=M/k;

            System.out.println("number of cluster samples for sampleClustering: "+ t);
            /*-----------------------------------------------------------------------------------------------

            we read the input and compute the size of each cluster, then saved the result in a
            BroadCast variable named sharedClusterSizes
            ---------------------------------------------------------------------------------------------- */

            //we modified the fullClustering RDD in switching the key-value (so the cluster number become the
            //then grouped by key with countByKey()

            Map<Integer, Long> cluster_sizes_map = currentClustering.flatMapToPair((document)->{
                Tuple2<Integer, Vector> pair = new Tuple2<>(document._2, document._1);

                ArrayList<Tuple2<Integer, Vector>> pairs = new ArrayList<>();

                pairs.add(pair);
                return pairs.iterator();

            }).countByKey();

            //needed to save the map in an array, used for our broadcast var
            Long[] array = new Long[clusters.k()];
            System.out.println("number of clusters : " + clusters.k());



            for(int x : cluster_sizes_map.keySet())
            {
                array[x]=cluster_sizes_map.get(x);
            }

            Broadcast<Long[]> sharedClusterSizes =sc.broadcast(array);
            //show clusters dimensions
            for(Long value: sharedClusterSizes.value())
                System.out.println(value +" ");
            System.out.println("");

            //create broadcast variable for the clusteringSample, used to compute the approx silhuette
            JavaPairRDD<Integer, Vector> mappa =  currentClustering.flatMapToPair((document)->{
                Tuple2<Integer, Vector> pair = new Tuple2<>(document._2, document._1);

                ArrayList<Tuple2<Integer, Vector>> pairs = new ArrayList<>();

                pairs.add(pair);
                return pairs.iterator();

            })
                    .groupByKey()
                    .flatMapToPair((elem)->{

                        Iterator<Vector> points = elem._2.iterator();
                        int cnt=0;
                        Random rand = new Random();
                        double point_prob;
                        ArrayList<Tuple2<Integer, Vector>> lista = new ArrayList<>();

                        while( points.hasNext())
                        {
                            point_prob = rand.nextDouble();
                            if (point_prob<Math.min((double)1,(double) t/sharedClusterSizes.value()[elem._1]))
                            {
                                lista.add(new Tuple2(elem._1, points.next()));
                            }
                            else points.next();
                        }

                        return lista.iterator();
                    });

            //OPERATIONS NEEDED to know the dimension of the clusters in clusteringSample
            Map<Integer, Long> aiuto = mappa.countByKey();
            List<Long> aiuto_sample = new ArrayList<>(mappa.countByKey().values());
            Broadcast<List<Long>> sample_clusters_size =sc.broadcast(aiuto_sample);//clusters dimensions
            System.out.println(sample_clusters_size.value());

            //NEEDED to convert our rdd "mappa" in List and then a
            //broadcast var clusteringSample as requested
            List<Tuple2<Integer, Vector>> sample = new ArrayList<>(mappa.countByValue().keySet());
            Broadcast<List<Tuple2<Integer, Vector>>> clusteringSample =sc.broadcast(sample);
           /* for(Tuple2<Integer, Vector> elem:clusteringSample.value())
            {
                System.out.println("* "+ elem._2+" --> " + elem._1);
            }*/

            start = System.currentTimeMillis();//start counting time

            //we compute the approximate silhouette of fullClustering points
            //MapPhase: we compute the silhouette for each point
            //ReducePhase: we compute the point silhouettes average
            double approxSilhFull = currentClustering.flatMapToPair((document)->{

                ArrayList<Tuple2<Integer, Double>> ris = new ArrayList<>();

                Vector point = document._1;

                double normalization;
                double sum=0;

                //array that will contains the consider point distances from each clusters
                ArrayList<Double> sums = new ArrayList<>();//set all 0's
                for(int i=0 ; i<sharedClusterSizes.value().length ; i++)
                    sums.add((double)0.0);

                //compute the overall distances
                for(Tuple2<Integer, Vector> x : clusteringSample.value())
                {
                    sums.set(x._1, sums.get(x._1) + (double)(Vectors.sqdist(x._2, point)));
                }

                //compute normalization factor and normalize
                for(int i=0; i<sums.size() ; i++)
                {
                    normalization= 1/(double)Math.min(t, sharedClusterSizes.value()[i]);
                    sums.set(i, normalization * sums.get(i) );
                }

                //take ap and bp from sums
                double ap = sums.remove((int)document._2);

                double bp = Collections.min(sums);

                //compute silhouette
                double sh = (bp-ap)/(double)Math.max(ap,bp);

                ris.add(new Tuple2(1, sh));
                return ris.iterator();
            }).groupByKey()
                    .flatMapToPair((elem)->{
                        ArrayList<Tuple2<Integer, Double>> avg = new ArrayList<>();

                        Iterator<Double> values = elem._2.iterator();

                        double mean=0.0;
                        int cnt=0;

                        //sum all the silhouettes and compute average
                        while(values.hasNext())
                        {
                            cnt++;
                            mean += values.next();
                        }

                        mean = mean/cnt;//overall average

                        avg.add(new Tuple2(1, mean));
                        return avg.iterator();
                    }).countByValue().keySet().iterator().next()._2;

            end = System.currentTimeMillis();//end counting time

            System.out.println("value of k: " + k);
            System.out.println("APPROXIMATION mean : " + approxSilhFull);
            System.out.println("time spent to clustering: " + clustering_time);
            System.out.println(" APPROXIMATION Elapsed time : " + (end - start) + " ms");
            System.out.println();
        }

    }
}
