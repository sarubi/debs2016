# [DEBS Grand Challenge 2016](http://www.ics.uci.edu/~debs2016/call-grand-challenge.html)
**The 10th ACM International Conference on Distributed and Event-Based Systems, Irvine, CA, June 20 - June 24, 2016**

The ACM DEBS 2016 Grand Challenge is the sixth in a series of challenges which seek to provide a common ground and uniform evaluation criteria for a competition aimed at both research and industrial event-based systems.
The goal of the 2016 DEBS Grand Challenge competition is to evaluate event-based systems for real-time analytics over high volume data streams in the context of graph models.

The underlying scenario addresses the analysis metrics for a dynamic (evolving) social-network graph. Specifically, the 2016 Grand Challenge targets following problems: 
(1) identification of the posts that currently trigger the most activity in the social network, and 
(2) identification of large communities that are currently involved in a topic.
The corresponding queries require continuous analysis of a dynamic graph under the consideration of multiple streams that reflect updates to the graph.


This repository contains the solution developed by Wso2 for the grand challenge using Siddhi.

**Inputs**

1. Path to friendship.dat file

2. Path to posts.dat file

3. Path to comments.dat file

4. Path to likes.dat file

5. Value of k for Query 2 (The number of top comments to print)

6. Value of d for Query 2 (The time period in which to analyse the large community)

**Outputs**

1. q1.txt (Contains Query 1 results)

2. q2.txt (Contains Query 2 results)

3. performance.txt (Contains 4 digits representing run time of query 1, average latency of query 1, run time of query 2 and average latency of query 2)


**How to build**

Run `mvn clean install`

**How to Run**

After building, you can just run the JAR file. 
Use `testCase1.sh` through `testCase7.sh` helper scripts to run the system for several given test data sets

Example: `./testCase1.sh`

Use the `run.sh` helper script to run the system by giving the data set as arguments

`./run.sh <path-to-friendships.dat> <path-to-posts.dat> <path-to-comments.dat> <path-to-likes.dat> <k> <d>`
