This project contains sample code for processing image documents using Tesseract software. 


### Pre-requisites

* Tesseract
* MapR 5.2 Cluster or Sandbox
* Java SDK 7 or newer
* Maven 3
* Elasticsearch

```
Launch MapR Sandbox 

NFS mount cluster file system on host 
mount maprdemo:/mapr/demo.mapr.com/user  /user
```


## Setup

Clone the repository, then

```
Make directory /user/user01/images
Copy sample data files in the repository to /user/user01/images

mvn clean install
```

Install and startup Elasticsearch:

```
Launch Elasticsearch
cd $ELASTIC_SEARCH_HOME 
./bin/elasticsearch
```

Create MapR-DB table and enable table replication:
```
Create datatable table
$ hbase shell
hbase(main):001:0> create '/user/user01/datatable', 'cf'

Create failed_documents table
hbase(main):001:0> create '/user/user01/failed_documents', 'cf'

Setup table replication
$ $MAPR_HOME/bin/register-elasticsearch -r localhost -e $ELASTIC_SEARCH_HOME -u mapr -y -c <elastic search node name>
$ $MAPR_HOME/bin/register-elasticsearch -l
$ $MAPR_HOME/bin/maprcli table replica elasticsearch autosetup -path /<srctable>  -target <elastic search node nam> -index <elastic search index>  -type json
```


Run the application:

```
mvn exec:java -Dexec.mainClass="com.mapr.ocr.text.ImageToText"
```

After execution is complete, look for the documents in Elasticsearch for keyword fox in the documents:

```
curl -XGET "http://maprdemo:9200/<index_name>/json/_search" -d'
{
   "query": {
      "match": {
     "cf.info": "quick"
      }},
      "fields" : ["_id", "cf.filepath", "cf.info"]
}' | python -m json.tool
```

