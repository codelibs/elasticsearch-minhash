Elasticsearch MinHash Plugin
=======================

## Overview

MinHash Plugin provides b-bit MinHash algorithm for Elasticsearch.
Using a field type and a token filter provided by this plugin, you can add a minhash value to your document.

## Version

[Versions in Maven Repository](https://repo1.maven.org/maven2/org/codelibs/elasticsearch-minhash/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-minhash/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### For Elasticsearch 5.x

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-minhash:5.3.0

### For Elasticsearch 2.x

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-minhash/2.3.1

## Getting Started

### Add MinHash Analyzer

First, you need to add a minhash analyzer when creating your index:

    $ curl -XPUT 'localhost:9200/my_index' -d '{
      "index":{
        "analysis":{
          "analyzer":{
            "minhash_analyzer":{
              "type":"custom",
              "tokenizer":"standard",
              "filter":["minhash"]
            }
          }
        }
      }
    }'

You are free to change tokenizer/char\_filter/filter settings, but the minhash filter needs to be added as a last filter.

### Add MinHash field

Put a minhash field into an index mapping:

    $ curl -XPUT "localhost:9200/my_index/my_type/_mapping" -d '{
      "my_type":{
        "properties":{
          "message":{
            "type":"string",
            "copy_to":"minhash_value"
          },
          "minhash_value":{
            "type":"minhash",
            "minhash_analyzer":"minhash_analyzer"
          }
        }
      }
    }'

The field type of minhash is of binary type.
The above example calculates a minhash value of the message field and stores it in the minhash\_value field.

## Get MinHash Value

Add the following document:

    $ curl -XPUT "localhost:9200/my_index/my_type/1" -d '{
      "message":"Fess is Java based full text search server provided as OSS product."
    }'

The minhash value is calculated automatically when adding the document.
You can check it as below:

    $ curl -XGET "localhost:9200/my_index/my_type/1?pretty&fields=minhash_value,_source" 

The response is:

    {
      "_index" : "my_index",
      "_type" : "my_type",
      "_id" : "1",
      "_version" : 1,
      "found" : true,
      "_source":{
          "message":"Fess is Java based full text search server provided as OSS product."
        },
      "fields" : {
        "minhash_value" : [ "KV5rsUfZpcZdVojpG8mHLA==" ]
      }
    }

## References

### Change the number of bits and hashes

To change the number of bits and hashes, set them to a token filter setting:

    $ curl -XPUT 'localhost:9200/my_index' -d '{
      "index":{
        "analysis":{
          "analyzer":{
            "minhash_analyzer":{
              "type":"custom",
              "tokenizer":"standard",
              "filter":["my_minhash"]
            }
          }
        },
        "filter":{
          "my_minhash":{
            "type":"minhash",
            "seed":100,
            "bit":2,
            "size":32
          }
        }
      }
    }'

The above allows to set the number of bits to 2, the number of hashes to 32 and the seed of hash to 100.

