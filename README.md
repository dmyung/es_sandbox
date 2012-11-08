es_sandbox
==========

Elasticsearch Testing
=====================

This is a set of documents and scripts to send to elasticsearch for testing between data inconsitencies we may encounter in our forms.

It's an attempt to proactively get ahead of the quirks of the elasticsearch index and mappings.

From the basic 001 json file, most types are defined in the given file. From then subsequent files try to deviate from the different types to see what's legal and what could cause problems on subsequent submissions to the same index+type.

Future iterations should focus on mappings defined and how they alter the tolerance of the index.

Additional type definitions for datetime deviations and formatting is also needed.

To run, just do python es_tester.py to see the results of the submissions.
