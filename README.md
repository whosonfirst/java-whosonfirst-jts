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


# Generating the regions.txt file

- check out the `whosonfirst-data/whosonfirst-data` Github project in the same folder as this repo.

  
    python src/main/python/generate_wof_url_lists.py



# Running the tests

    .gradlew test
    open build/reports/tests/test/index.html