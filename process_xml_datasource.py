import simplejson, subprocess, sys, imp, multiprocessing, os, logging
from lxml import etree as ET
from argparse import ArgumentParser

FORMAT = '[%(asctime)-15s] [%(process)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.level = logging.DEBUG

def load_schema(path):
    try:
        schema_module = imp.load_source("schema_module", path)
        return schema_module.schema
    except AttributeError:
        raise Exception, "Cannot import schema from path specified"

# count the number of characters in the file at path *filename*
def character_count(filename):
    output = subprocess.check_output(["wc", "-m", filename])
    return int(output.strip().split()[0])

def get_value_for_field(el, properties):
    if 'attribute' in properties:
        return el.attrib.get(properties['attribute'])

    else:
        return el.text

def element_path(el):
    tags = [el.tag]

    while el.getparent() is not None:
        el = el.getparent()
        tags.insert(0, el.tag)

    return '/'.join(tags)

def find_by_absolute_path(el, path):
    while el.getparent() is not None:
        current_path = element_path(el)
        if path.startswith(current_path):
            rest_of_path = path.split(current_path)[1].strip('/')
            return el.findall(rest_of_path)
        el = el.getparent()

def find_by_absolute_path(el, path):
    while el.getparent() is not None:
        current_path = element_path(el)
        if path.startswith(current_path):
            rest_of_path = path.split(current_path)[1].strip('/')
            return el.find(rest_of_path)
        el = el.getparent()

# perform topological sort on the fields to determine the order of processing
def order_fields(fields):
    logger.debug("Ordering fields based on dependencies")

    no_dependencies = set()
    edges = {}

    for name, properties in fields.items():
        dependencies = properties.get('dependencies', [])

        if len(dependencies) == 0: no_dependencies.add(name)

        for dependency in dependencies:
            if not (dependency in edges): edges[dependency] = set()
            edges[dependency].add(name)

    ordered = []

    while len(no_dependencies) > 0:
        field = no_dependencies.pop()
        ordered.append(field)

        dependents = list(edges.get(field, set()))

        for dependent in dependents:
            edges[field].remove(dependent)

            has_no_more_dependencies = True
            for _, outgoing in edges.items():
                if dependent in outgoing:
                    has_no_more_dependencies = False

            if has_no_more_dependencies:
                no_dependencies.add(dependent)

    return map(lambda name: (name, fields[name]), ordered)

# process every source element *el* according to the schema fields *output_fields*
def process_element(el, output_fields):
    obj = {}

    for name, properties in output_fields:
        field_type = properties.get("type", str)
        is_list = type(field_type) is list

        if is_list:
            if 'absolute_path' in properties:
                absolute_path = properties['absolute_path']
                relevant_elements = findall_by_absolute_path(el, absolute_path)

            elif 'path' in properties:
                path = properties['path']
                relevant_elements = el.findall(path)

            else:
                relevant_elements = []

            if 'filter' in properties:
                filter_f = properties['filter']
                relevant_elements = filter(filter_f, relevant_elements)

            field_value = map(lambda e: get_value_for_field(e, properties), relevant_elements)
            field_value = filter(lambda x: x is not None, field_value)

            if field_type[0] != str:
                field_value = map(field_type[0], field_value)

        else:
            if 'absolute_path' in properties:
                absolute_path = properties['absolute_path']
                relevant_element = find_by_absolute_path(el, absolute_path)

            elif 'path' in properties:
                path = properties['path']
                relevant_element = el.find(path)

            else:
                relevant_element = None

            if relevant_element is not None:
                field_value = get_value_for_field(relevant_element, properties)

                # if field type is string, no casting needed
                if field_type != str:
                    field_value = field_type(field_value)

            else:
                field_value = None

        if 'transform' in properties:
            transform_f = properties["transform"]

            # grab the values of the dependencies
            dependencies = properties.get('dependencies', [])
            dependencies_values = map(lambda dep: obj[dep], dependencies)

            # make sure that we have all the values
            if all(v is not None for v in dependencies_values):
                field_value = transform_f(field_value, dependencies_values)

        obj[name] = field_value

    return obj

# transform XML file *filename* according to schema *schema* and write the result to *output_filename*
def process_file(filename, schema_path, output_filename, queue = None):
    if output_filename is None:
        output = sys.stdout
    else:
        output = open(output_filename, 'w')

    schema = load_schema(schema_path)
    iterator = ET.iterparse(open(filename))
    ordered_fields = order_fields(schema['fields'])

    logger.debug("Extracting data from %s" % filename)

    while True:
        try:
            event, el = iterator.next()
        except StopIteration:
            break

        if element_path(el) == schema['path']:
            entry = process_element(el, ordered_fields)
            json_string = simplejson.dumps(entry) + "\n"

            if queue:
                queue.put(json_string)
            else:
                output.write(json_string)

            el.clear()

def writer(queue, output_filename):
    with open(output_filename, 'w') as f:
        while True:
            line = queue.get()
            if line == "END": return
            f.write(line)

def parallel_process_files(filenames, schema_path, output_filename):
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    # create a single process to write to output in order to avoid conflicts
    process = multiprocessing.Process(target = writer, args = (queue, output_filename))
    process.start()

    logger.debug("Processing each part in parallel...")
    # create a process for every XML part
    p = multiprocessing.Pool(len(filenames))
    for index, filename in enumerate(filenames):
        p.apply_async(process_file, (filename, schema_path, None, queue))
    p.close()
    p.join()

    # end the writer process
    queue.put("END")

def clone_xml_element(e):
    return ET.fromstring(ET.tostring(e))

def remove_all_children(e):
    for child in e.iterchildren():
        e.remove(child)

def remove_all_children_except(e, exception):
    for child in e.iterchildren():
        if child is not exception:
            e.remove(child)

def wrap_around_xml_element(el):
    parent = clone_xml_element(el.getparent())
    remove_all_children(parent)
    placeholder = ET.Element("placeholder")
    parent.append(placeholder)

    while parent.getparent():
        remove_all_children_except(parent.getparent(), parent)
        parent = parent.getparent()

    xml_string = ET.tostring(parent, xml_declaration = True)
    return xml_string.split("<placeholder/>")

# splits XML file at *filename* to (approximately) *n* parts, splitting on path *schema_path*
def split_xml_file(filename, schema_path, n):
    schema = load_schema(schema_path)
    split_path = schema['split_path']

    logger.debug("Calculating part size")

    size = character_count(filename)
    part_size = size/n

    iterator = ET.iterparse(open(filename), events = ('start', 'end'))
    element_tree = []
    part_count = 0
    part_filename = "%s.0" % filename
    current_part_file = open(part_filename, 'w')
    part_filenames = [part_filename]
    started_writing = False

    while True:
        try:
            event, el = iterator.next()
        except StopIteration:
            break

        current_path = element_path(el)

        if current_path == split_path:
            if not started_writing:
                xml_beginning, _ = wrap_around_xml_element(el)
                current_part_file.write(xml_beginning)
                started_writing = True

            current_part_file.write(ET.tostring(el))

            el.clear()

            if current_part_file.tell() > part_size:
                _, xml_end = wrap_around_xml_element(el)
                current_part_file.write(xml_end)

                part_count += 1
                part_filename = "%s.%d" % (filename, part_count)

                part_filenames.append(part_filename)
                logger.debug("Wrote part %d to %s" % (part_count, part_filename))

                current_part_file = open(part_filename, 'w')

                xml_beginning, _ = wrap_around_xml_element(el)
                current_part_file.write(xml_beginning)

    part_count += 1
    current_part_file.write(xml_end)

    logger.debug("Wrote part %d to %s" % (part_count, part_filename))

    return part_filenames

def split_and_process(filename, schema_path, output_filename, n_parts, keep = False):
    logger.debug("Splitting %s to %d parts" % (filename, n_parts))
    filenames = split_xml_file(filename, schema_path, n_parts)
    parallel_process_files(filenames, schema_path, output_filename)
    if not keep:
        for fn in filenames:
            logger.debug("Deleting %s" % fn)
            os.remove(fn)

if __name__ == "__main__":
    parser = ArgumentParser("Tool for trasforming a XML datasource into JSON documents according to a specified schema")
    parser.add_argument('input', type = str, help = "Path to XML input file")
    parser.add_argument('-s', '--schema', type=str, help = "Path to schema file", default = "schema.py")
    parser.add_argument('-o', '--output', type=str, help = "Path to output JSON file")
    parser.add_argument('-n', '--n-parts', type=int, help = "Number of parts to split the XML file for parallel processing")
    parser.add_argument('--keep-parts', action='store_true', default=False, help = "Keep parts after splitting XML input (for debug)")

    args = parser.parse_args()

    if args.n_parts:
        split_and_process(args.input, args.schema, args.output, args.n_parts, args.keep_parts)

    else:
        process_file(args.input, args.schema, args.output)
