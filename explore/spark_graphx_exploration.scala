/*
***********************************************************************
*
* LICENSE: This license requires that reusers give credit to the creator.
* It allows reusers to distribute, remix, adapt, and build upon the
* material in any medium or format, for noncommercial purposes only. 

* @Title: Shareholding Relationship Graph of Tehran Stock Exchange
* @Author: Mobin Ranjbar
* @Email: mobinranjbar1[at]gmail[dot]com
* @License: CC BY-NC 4.0 (Attribution-NonCommercial 4.0 International)
*
********** Documentation **********************************************
*
* The Tehran Stock Exchange (TSE) is Iran's largest stock exchange, 
* which first opened in 1967. As of May 2023, 666 companies with a
* combined market capitalization of US$1.45 trillion were listed on
* TSE. Iran's capital market has companies from a wide range of
* industries,including automotive, telecommunications, agculture,
* petrochemical, mining, steel iron, copper, banking and insurance,
* banking and others. Many of the companies listed are state-owned
* firms that have been privatized. (WikiPedia)
*
* Each company that is registered in TSE have their own (private/public)
* shareholders that you can access the information about them publicly
* on TSETMC website. In this project, I used this data to build the
* relationship graph of shareholders using data science and big data
* tools e.g. Apache Spark, MongoDB, Neo4j.
*
* DISCLAIMER: The author of this project did not use any confidential or
* leaked data in the market and It is not responsible for the misuse
* of it. This project has been developed only for research purposes
* and the owner of the data has its rights to request for deletion.
*
* Any sort of contribution is welcome!
*
* Open an issue if you find a bug.
*
* Contact me if you like it.
*
***********************************************************************
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

val working_directory_path = "file:///home/mobin/SRG_TSE"
val NEO4J_URL="neo4j://localhost:7687"
val NEO4J_USERNAME="neo4j"
val NEO4J_PASSWORD="123456"

val stocks: RDD[(VertexId,(String,String))] = spark.read.format("org.neo4j.spark.DataSource")
.option("url", NEO4J_URL)
.option("authentication.basic.username", NEO4J_USERNAME)
.option("authentication.basic.password", NEO4J_PASSWORD)
.option("labels","Stock").load().rdd
.map(row => (row.getAs[Long]("<id>"), (row.getAs[String]("title"),row.getAs[String]("stock_id"))))

val shareholders: RDD[(VertexId,(String,String))] = spark.read.format("org.neo4j.spark.DataSource")
.option("url", NEO4J_URL)
.option("authentication.basic.username", NEO4J_USERNAME)
.option("authentication.basic.password", NEO4J_PASSWORD)
.option("labels","Shareholder").load().rdd
.map(row => (row.getAs[Long]("<id>"), (row.getAs[String]("title"),row.getAs[String]("real_name"))))

val related: RDD[Edge[String]] = spark.read.format("org.neo4j.spark.DataSource")
.option("url", NEO4J_URL)
.option("authentication.basic.username", NEO4J_USERNAME)
.option("authentication.basic.password", NEO4J_PASSWORD)
.option("relationship.nodes.map", "false")
.option("relationship", "RELATED")
.option("relationship.source.labels", "Stock")
.option("relationship.target.labels", "Shareholder").load().rdd
.map(row => Edge(row.getAs[Long]("<source.id>"), row.getAs[Long]("<target.id>"), row.getAs[String]("<rel.type>")))

val graph = Graph(stocks, related)

print("The Graph Vertices Count: "+graph.vertices.count()+"\n")
print("The Graph Edges Count: "+graph.edges.count()+"\n")

val cc = graph.connectedComponents().vertices

val page_rank = PageRank.run(graph, 5).vertices

graph.triplets.saveAsTextFile(working_directory_path+"/results/graphx_graph_output")

page_rank.saveAsTextFile(working_directory_path+"/results/graphx_pagerank_result")

cc.saveAsTextFile(working_directory_path+"/results/graphx_connectedcomponents_result")