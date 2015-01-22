A tool for transforming a XML datasource into JSON documents according to a specified schema.

# Installation

    $ sudo pip install lxml simplejson

# Schema

The script uses a simple DSL to specify the fields to extract from the input XML to produce the output. An example of a schema can be found at `schema.py`.

# Usage

To process an XML datasource `input.xml` according schema `schema.py` to produce `output.json`:

    python process_xml_datasource.py input.xml -s schema.py -o output.json

To split the same input to 3 parts and process them in parallel:

    python process_xml_datasource.py input.xml -n 3 -s schema.py -o output.json

To verify that the input and output have the same number of records:

    python verify_JSON.py input.xml output.json
