Elasticsearch MinHash Plugin
=======================

## Overview

MinHash Plugin provides b-bit MinHash algorism for Elasticsearch.
Using a field type and a token filter provided by this plugin, you can add a minhash value to your document.

## Version

| Version   | Elasticsearch |
|:---------:|:-------------:|
| master    | 2.2.X         |
| 2.2.0     | 2.2.0         |
| 2.1.0     | 2.1.1         |
| 1.4.1     | 1.4.0.Beta1   |
| 1.3.0     | 1.3.2         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-minhash/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install QRCache Plugin

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-minhash/2.2.0

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

You can feel free to change tokenizer/char\_filter/filter settings, but minhash filter needs to be added as a last filter.

### Add MinHash field

Put minhash field into an index mapipng:

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

The field type of minhash is a binary type.
The above example is to calculate a minhash value of message field and store it to minhash\_value field.

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

The above is, the number of bits is 2, the number of hashes is 32 and a seed of hash is 100.

