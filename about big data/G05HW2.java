import com.beust.jcommander.internal.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.SparkCuratorUtil;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;


import java.util.*;

public class G05HW2 {



    /*
    provided function for process a line of the read input file
     */
    public static Tuple2<Vector, Integer> strToTuple (String str){
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length-1; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        Vector point = Vectors.dense(data);
        Integer cluster = Integer.valueOf(tokens[tokens.length-1]);
        Tuple2<Vector, Integer> pair = new Tuple2<>(point, cluster);
        return pair;
    }

    public static void main(String[] args)
    {
        //parameters check
        if (args.length != 3)
        {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }


        // SPARK SETUP

        //spark configuration, give a name to the app
        SparkConf conf = new SparkConf(true).setAppName("ReviewApp");

        //set the sparkConfig=true means that we can give commandos to spark config
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");//"re-call" method that decide the number of warning we want to see during the run


        // INPUT READING

        // Read file name
        String file_name = args[0];

        //number of clusters
        int num_cluster = Integer.parseInt(args[1]);

        //t number (points to pick from each cluster in order to obtain sampleClustering
        int t = Integer.parseInt(args[2]);

        /*
        To read the input text file (e.g., inputPath) containing a clustering
        into the RDD fullClustering do:

        -the number of partition in set to 8
         */
        JavaPairRDD<Vector,Integer> fullClustering = sc.textFile(file_name)
                .mapToPair(x -> strToTuple(x)).repartition(8).cache();

        /*-----------------------------------------------------------------------------------------------
        POINT 2
        we read the input and compute the size of each cluster, then saved the result in a
        BroadCast variable named sharedClusterSizes
        ---------------------------------------------------------------------------------------------- */

        //we modified the fullClustering RDD in switching the key-value (so the cluster number become the
        //then grouped by key with countByKey()

       Map<Integer, Long> cluster_sizes_map = fullClustering.flatMapToPair((document)->{
            Tuple2<Integer, Vector> pair = new Tuple2<>(document._2, document._1);

            ArrayList<Tuple2<Integer, Vector>> pairs = new ArrayList<>();

            pairs.add(pair);
            return pairs.iterator();

        }).countByKey();

        //needed to save the map in an array, used for our broadcast var
        Long[] array = new Long[num_cluster];
        for(int x : cluster_sizes_map.keySet())
        {
            array[x]=cluster_sizes_map.get(x);
        }

        Broadcast<Long[]> sharedClusterSizes =sc.broadcast(array);//??

        /*------------------------------------------------------------------------
        POINT 3
        we created the sampleClustering broadCast variable.
        For each point we keep it only if the random double was less than t/point_cluster_size
        in order to pick each with prob t/cluster_size as requested.
        The whole operation is done in only one round:
        -mapPhase: we switch key and value
        groupedByKey
        -reducePhase: pick point w. poisson sampling prob
        ---------------------------------------------------------------------*/
        JavaPairRDD<Integer, Vector> mappa =  fullClustering.flatMapToPair((document)->{
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

        //NEEDED to convert our rdd "mappa" in List and then a
        //broadcast var clusteringSample as requested
        List<Tuple2<Integer, Vector>> sample = new ArrayList<>(mappa.countByValue().keySet());
        Broadcast<List<Tuple2<Integer, Vector>>> clusteringSample =sc.broadcast(sample);

        long start = System.currentTimeMillis();//start counting time

        //we compute the approximate silhouette of fullClustering points
        //MapPhase: we compute the silhouette for each point
        //ReducePhase: we compute the point silhouettes average
        double approxSilhFull = fullClustering.flatMapToPair((document)->{

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

        long end = System.currentTimeMillis();//end counting time

        System.out.println(" APPROXIMATION Elapsed time : " + (end - start) + " ms");
        System.out.println("APPROXIMATION mean : " + approxSilhFull);

        //POINT 5
        //exact silhouette computed sequentially

        double exactSilhSample =0.0;
        double point_silhuette, ap, bp, normalization;

        //to store all the sums
        ArrayList<Double> cluster_sums = new ArrayList<>();
        for(int i=0 ; i<num_cluster ; i++)
        {
            cluster_sums.add(0.0);
        }

        start = System.currentTimeMillis();
        for(Tuple2<Integer, Vector> x : clusteringSample.value())
        {
            //reset to all 0's
            cluster_sums = new ArrayList<>();
            for(int i=0 ; i<num_cluster ; i++)
            {
                cluster_sums.add(0.0);
                //cluster_sums.set(i,0.0);
            }

            point_silhuette=0.0;//reset to 0

            //compute distances
            for(Tuple2<Integer, Vector> y : clusteringSample.value())
            {
                double dist=(double)Vectors.sqdist(x._2,y._2);
                cluster_sums.set(y._1, cluster_sums.get(y._1) + dist);
            }

            //compute and do normalization
            for(int i=0; i<num_cluster ; i++)
            {
                normalization= 1/(double)sample_clusters_size.value().get(i);
                cluster_sums.set(i, normalization * cluster_sums.get(i) );
            }

            //compute silhouette
            ap=cluster_sums.remove((int)x._1);

            bp = (double)Collections.min(cluster_sums);

            point_silhuette = (bp-ap)/(double)Math.max(ap,bp);

            exactSilhSample = (exactSilhSample + point_silhuette);
        }

        //compute average silhouette
        exactSilhSample = exactSilhSample/clusteringSample.value().size();
        end = System.currentTimeMillis();

        System.out.println("------------------------------------------------------");
        System.out.println(" EXACT Elapsed time : " + (end - start) + " ms");
        System.out.println("EXACT mean : " + exactSilhSample);

        /*
        CLARIFICATION

        Looking at the table (of times and coefficients) it can be seen that with the dataset 3 small,
        with t = 1000 the silhouette approx and exact are not the same.
        Given the anomaly (they should be the same since fullClustering and sampleClustering for t = 1000 contain the same elements)
        we tried to calculate the exact silhouettes using an RDD, as done for the approximate,
        and in this case the accounts add up, that is, in case 3 small with t = 1000 the two coefficients coincide.
        We have checked several times the code used for the calculation of the exact sequential silhouette
        without understanding the error reason for this inconsistency.

        For this reason we decided to leave the code used to calculate the exact silhouette below
        */


        start = System.currentTimeMillis();

        //EXACT SILHUETTE
        double silh = mappa.flatMapToPair((document)->{


            ArrayList<Tuple2<Integer, Double>> ris = new ArrayList<>();

            Vector point = document._2;
            //double sum=0;

            ArrayList<Double> sums = new ArrayList<>();//set all 0's
            for(int i=0 ; i<sample_clusters_size.value().size() ; i++)
                sums.add((double)0.0);

            for(Tuple2<Integer, Vector> x : clusteringSample.value())
            {
                //if( x._1== document._2)
                sums.set(x._1, sums.get(x._1) + (double)(Vectors.sqdist(x._2, point)));
            }
            double norm;

            //normalize
            for(int i=0; i<sums.size() ; i++)
            {
                norm= 1/(double)sample_clusters_size.value().get(i);
                sums.set(i, norm * sums.get(i) );
            }

            //take ap and bp from sums
            double ap_coeff = sums.remove((int)document._1);

            double bp_coeff = Collections.min(sums);

            //shiluette
            double sh = (bp_coeff-ap_coeff)/(double)Math.max(ap_coeff,bp_coeff);

            ris.add(new Tuple2(1, sh));
            return ris.iterator();
        }).groupByKey()
                .flatMapToPair((elem)->{
                    ArrayList<Tuple2<Integer, Double>> avg = new ArrayList<>();

                    Iterator<Double> values = elem._2.iterator();

                    double mean=0.0;
                    int cnt=0;

                    while(values.hasNext())
                    {
                        cnt++;
                        mean += values.next();
                    }

                    mean = mean/cnt;

                    avg.add(new Tuple2(1, mean));
                    return avg.iterator();
                }).countByValue().keySet().iterator().next()._2;
        end = System.currentTimeMillis();

        System.out.println("----------------------------------------------------------");
        System.out.println(" EXACT Elapsed time (RDD) : " + (end - start) + " ms");
        System.out.println("EXACT mean (RDD) : " + silh);

    }
}

