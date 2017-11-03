# A testing project for Whos-on-first

Java tools for working with Who's On First documents and the Java Topology Suite (JTS)

This repo is testing the validity of shapes from https://github.com/whosonfirst-data/whosonfirst-data

The libraries used are the ones that Elasticsearch uses to parse shapes, hopefully we by this can make the shapes more indexing-friendly.

The data is retrieved via URls in the `region.txt` file for all regions in WOF (to start with).

## Contributors

* @peterneubauer

## See also

* https://github.com/whosonfirst-data/whosonfirst-data/issues/975
* https://sourceforge.net/projects/jts-topo-suite/
* https://github.com/locationtech/spatial4j


# Installing a local elassticsearch server

    docker run -p 9200:9200 -p 9300:9300  -e "xpack.security.enabled=false" -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:5.6.3
    curl -X PUT http://localhost:9200/wofs/ 
    curl -X PUT http://localhost:9200/wofs/_mapping/wof -d'@src/test/resources/els_mappings/wof.json'


# Importing shapes into a Elasticsearch server

    ELS_MAIN2_HOST=localhost ELS_MAIN2_PORT=9200 python src/main/python/import_wof_shapes.py

# Generating the regions.txt file

- check out the `whosonfirst-data/whosonfirst-data` Github project in the same folder as this repo.


    python src/main/python/generate_wof_url_lists.py


# Running the java jts tests

    .gradlew test
    open build/reports/tests/test/index.html