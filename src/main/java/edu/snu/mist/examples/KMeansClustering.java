/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.snu.mist.examples;

import edu.snu.mist.api.APIQueryControlResult;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Example client which submits a query doing k-means clustering.
 */
public final class KMeansClustering {

  /**
   * Submit a query doing k-means clustering.
   * The query receives inputs in a form of two-dimensional double point "x1.x2,y1.y2",
   * conducts online k-means clustering, and displays the result of clustering.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   */
  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final String sourceSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NettySourceAddress.class);
    final SourceConfiguration localTextSocketSourceConf =
        MISTExampleUtils.getLocalTextSocketSourceConf(sourceSocket);

    final MISTFunction<List<Cluster>, List<String>> flatMapFunc =
        // parse clustering result into String list
        (clusterList) -> {
          final List<String> results = new LinkedList<>();
          for (final Cluster cluster : clusterList) {
            String clusterResult = "Cluster id: " + cluster.getId() +
                ", cluster center: " + cluster.getCenter().toString() + "\n";
            for (final Point point : cluster.getClusteredPoints()) {
              clusterResult += point.toString() + "\n";
            }
            results.add(clusterResult);
          }
          return results;
        };
    final ApplyStatefulFunction<Point, List<Cluster>> applyStatefulFunction = new KMeansFunction();

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder("example-group", "user1");
    queryBuilder.socketTextStream(localTextSocketSourceConf)
        // remove all space in the input string
        .map(s -> s.replaceAll(" ", ""))
            // filter inputs with two-dimensional double point form
        .filter(s -> Pattern.matches("-?\\d+\\.\\d+,-?\\d+\\.\\d+", s))
            // map string input into Point
        .map(s -> {
          final String[] array = s.split(",");
          return new Point(Double.parseDouble(array[0]), Double.parseDouble(array[1]));
        })
            // apply online k-means clustering to the input point
        .applyStateful(applyStatefulFunction)
            // flat the cluster list into multiple string output
        .flatMap(flatMapFunc)
            // display the result
        .textSocketOutput(MISTExampleUtils.SINK_HOSTNAME, MISTExampleUtils.SINK_PORT);
    final MISTQuery query = queryBuilder.build();

    return MISTExampleUtils.submit(query, configuration);
  }

  /**
   * Set the environment(Hostname and port of driver, source, and sink) and submit a query.
   * @param args command line parameters
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    final CommandLine commandLine = MISTExampleUtils.getDefaultCommandLine(jcb)
        .registerShortNameOfClass(NettySourceAddress.class) // Additional parameter
        .processCommandLine(args);

    if (commandLine == null) {  // Option '?' was entered and processCommandLine printed the help.
      return;
    }

    Thread sinkServer = new Thread(MISTExampleUtils.getSinkServer());
    sinkServer.start();

    final APIQueryControlResult result = submitQuery(jcb.build());
    System.out.println("Query submission result: " + result.getQueryId());
  }

  private KMeansClustering() {
  }

  /**
   * The state managing function of K-means clustering used during the aggregating operation on window.
   * The clustering algorithm used in this class is based on next references:
   * E. Liberty, R. Sriharsha, and M. Sviridenko. An algorithm for online k-means clustering. arXiv:1412.5721v2, 2014.
   * D. Sculley. Web-scale k-means clustering. in Proc. 19th Int. Conf. World Wide Web, Raleigh, NC, USA, 2010.
   */
  private static final class KMeansFunction implements ApplyStatefulFunction<Point, List<Cluster>> {
    // The number of received inputs
    private int n;
    // The new cluster count for a single facility cost
    private int qr;
    // The facility cost that used to determine to create a new cluster or not
    private double fr;
    // The collection of clusters that contain the center and input points of it
    private Collection<Cluster> clusters;

    private KMeansFunction() {
    }

    @Override
    public void initialize() {
      n = 0;
      qr = 0;
      fr = 0;
      clusters = new LinkedList<>();
    }

    /**
     * Conducts clustering for a new point input.
     * This method creates a new cluster for the new point or update an existing cluster with it,
     * according to the distance from the new point to nearest cluster.
     * @param newPoint the incoming point
     */
    @Override
    public void update(final Point newPoint) {
      n++;
      switch (n) {
        case 2: {
          final Point firstP = clusters.iterator().next().getClusteredPoints().iterator().next();
          fr = newPoint.distPowTwo(firstP) / 2;
        }
        case 1: {
          clusters.add(new Cluster(newPoint, n));
          break;
        }
        default: {
          // Finds nearest cluster from new input point
          Cluster nearestCluster = null;
          double minDistance = Double.MAX_VALUE;
          for (final Cluster cluster : clusters) {
            final double distFromCluster = newPoint.distPowTwo(cluster.getCenter());
            if (distFromCluster <= minDistance) {
              minDistance = distFromCluster;
              nearestCluster = cluster;
            }
          }
          if (Math.random() < minDistance / fr) {
            // Creates new cluster
            clusters.add(new Cluster(newPoint, clusters.size() + 1));
            qr++;
          } else {
            // Extends the nearest cluster to new input point
            nearestCluster.update(newPoint);
          }
          if (qr >= 3 * (1 + Math.log(n))) {
            // Increases cluster creation threshold
            qr = 0;
            fr *= 2;
          }
        }
      }
    }

    @Override
    public Collection<Cluster> getCurrentState() {
      return clusters;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setFunctionState(final Object loadedState) throws RuntimeException {
      clusters = (Collection<Cluster>) loadedState;
    }

    /**
     * @return the cluster information
     */
    @Override
    public List<Cluster> produceResult() {
      return new LinkedList<>(clusters);
    }
  }

  /**
   * Point class represents a point that consists of two double value.
   */
  private static final class Point {
    private double x;
    private double y;

    public Point(final double x, final double y) {
      this.x = x;
      this.y = y;
    }

    /**
     * Calculates (distance from this point to another point p) ^ 2.
     * @param p the another point to calculate distance
     * @return the calculated result
     */
    public double distPowTwo(final Point p) {
      return Math.pow(x - p.getX(), 2) + Math.pow(y - p.getY(), 2);
    }

    /**
     * @return the Point represented in String form
     */
    public String toString() {
      return x + ", " + y;
    }

    public double getX() {
      return x;
    }

    public void setX(final double x) {
      this.x = x;
    }

    public double getY() {
      return y;
    }

    public void setY(final double y) {
      this.y = y;
    }
  }

  /**
   * Cluster class represents a cluster information which contains it's center and points in it.
   */
  private static final class Cluster {
    // The center of this cluster
    private final Point center;
    // The id of this cluster
    private final int id;
    // The collection of all points in this cluster
    private final Collection<Point> clusterPoints;

    public Cluster(final Point center, final int id) {
      this.center = center;
      this.id = id;
      this.clusterPoints = new LinkedList<>();
      clusterPoints.add(center);
    }

    /**
     * Updates the cluster to contain a new point p.
     * @param p the new point
     */
    public void update(final Point p) {
      clusterPoints.add(p);
      final double constant = 1 / clusterPoints.size();
      center.setX((1 - constant) * center.getX() + constant * p.getX());
      center.setY((1 - constant) * center.getY() + constant * p.getY());
    }

    public Point getCenter() {
      return center;
    }

    public Collection<Point> getClusteredPoints() {
      return clusterPoints;
    }

    public int getId() {
      return id;
    }
  }
}