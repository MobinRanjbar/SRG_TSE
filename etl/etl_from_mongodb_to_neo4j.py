# coding=utf-8
#######################################################################
#
# LICENSE: This license requires that reusers give credit to the creator.
# It allows reusers to distribute, remix, adapt, and build upon the
# material in any medium or format, for noncommercial purposes only. 

# @Title: Shareholding Relationship Graph of Tehran Stock Exchange
# @Author: Mobin Ranjbar
# @Email: mobinranjbar1[at]gmail[dot]com
# @License: CC BY-NC 4.0 (Attribution-NonCommercial 4.0 International)
#
########## Documentation ##############################################
#
# The Tehran Stock Exchange (TSE) is Iran's largest stock exchange, 
# which first opened in 1967. As of May 2023, 666 companies with a
# combined market capitalization of US$1.45 trillion were listed on
# TSE. Iran's capital market has companies from a wide range of
# industries,including automotive, telecommunications, agriculture,
# petrochemical, mining, steel iron, copper, banking and insurance,
# banking and others. Many of the companies listed are state-owned
# firms that have been privatized. (WikiPedia)
#
# Each company that is registered in TSE have their own (private/public)
# shareholders that you can access the information about them publicly
# on TSETMC website. In this project, I used this data to build the
# relationship graph of shareholders using data science and big data
# tools e.g. Apache Spark, MongoDB, Neo4j.
#
# DISCLAIMER: The author of this project did not use any confidential or
# leaked data in the market and It is not responsible for the misuse
# of it. This project has been developed only for research purposes
# and the owner of the data has its rights to request for deletion.
#
# Any sort of contribution is welcome!
#
# Open an issue if you find a bug.
#
# Contact me if you like it.
#
#######################################################################

from neo4j import GraphDatabase
from pymongo import MongoClient
import configparser
import pathlib
from datetime import datetime
import os
os.environ["PYTHONIOENCODING"] = "utf-8"

start_time = datetime.now()

try:
    config = configparser.ConfigParser()
    config_path = pathlib.Path(__file__).parent.absolute() / "../config/config.ini"
    config.read(config_path)
except:
    print("Error: config.ini not found.")
    exit(0)

MONGODB_HOST = config['MONGODB']['mongodb_host']
MONGODB_PORT = int(config['MONGODB']['mongodb_port'])
MONGODB_USERNAME = config['MONGODB']['mongodb_username']
MONGODB_PASSWORD = config['MONGODB']['mongodb_password']
MONGODB_DBNAME = config['MONGODB']['mongodb_dbname']
MONGODB_COLLECTION_NAME = config['MONGODB']['mongodb_collection_name']

NEO4J_HOST = config['NEO4J']['neo4j_host']
NEO4J_PORT = int(config['NEO4J']['neo4j_port'])
NEO4J_USERNAME = config['NEO4J']['neo4j_username']
NEO4J_PASSWORD = config['NEO4J']['neo4j_password']

client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT,username=MONGODB_USERNAME,password=MONGODB_PASSWORD)
db = client[MONGODB_DBNAME]
ownership = db[MONGODB_COLLECTION_NAME]

shareholders_list = list(ownership.find({},{"_id":0}))

ite = 0
for i,row in enumerate(shareholders_list):
    stock_id = str(row["stock_id"])
    stock_name = row["stock_symbol"]
    print("Progress: "+str(i+1)+" of "+str(len(shareholders_list)))
    print("StockId: "+str(stock_id))
    print("Stock Name: "+str(stock_name))
    ite+=1
    con = GraphDatabase.driver("neo4j://{}:{}".format(NEO4J_HOST,NEO4J_PORT), auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    con_session = con.session()
    query = "create (s{}:Stock{{title:'{}',stock_id:'{}'}});".format(stock_id,stock_name,stock_id)
    con_session.run(query)
    for r in row["shareholders"]:
        shareholder_name = r["name"]
        if ite == 1:
            query = "match (n) where n.title='{}' create (sh{}:Shareholder{{title:'{}',real_name:'{}'}})-[:RELATED]->(n);".format(stock_name,str(r['Sid']),str(r['Sid']),shareholder_name)
            con_session.run(query)
        else:
            query = "MATCH (n:Shareholder{{title:'{}',real_name:'{}'}}) match (a:Stock{{title:'{}',stock_id:'{}'}}),(c:Shareholder{{title:'{}',real_name:'{}'}}) create (c)-[:RELATED]->(a) WITH count(n{{title:'{}'}}) as nodesAltered WHERE nodesAltered = 0 MATCH (n:Stock{{title:'{}',stock_id:'{}'}}) create (n)-[:RELATED]->(sh{}:Shareholder{{title:'{}',real_name:'{}'}});".format(str(r['Sid']),shareholder_name,stock_name,stock_id,str(r['Sid']),shareholder_name,str(r['Sid']),stock_name,stock_id,str(r['Sid']),str(r['Sid']),shareholder_name)
            con_session.run(query)
    con_session.close()
    con.close()

client.close()
end_time = datetime.now()
total_elapsed = end_time - start_time
print("The script which started at "+str(start_time)+" ended at: "+str(end_time)+"\n")
print("Elapsed seconds: "+str(total_elapsed.seconds))
