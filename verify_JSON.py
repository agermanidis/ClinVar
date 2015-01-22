import subprocess, sys
from lxml import etree as ET
from argparse import ArgumentParser

def line_count(filename):
    output = subprocess.check_output(["wc", "-l", filename])
    return int(output.strip().split()[0])

def count_elements_with_path(filename, path):
    iterator = ET.iterparse(open(filename), events = ('start', 'end'))
    element_tree = []
    count = 0

    while True:
        try:
            event, el = iterator.next()
        except StopIteration:
            break

        if event == 'start':
            element_tree.append(el)

        else:
            current_path = '/'.join(map(lambda e: e.tag, element_tree))

            if current_path == path:
                count += 1

            element_tree.pop()
            el.clear()

    return count

def run_test(args):
    input_filename = args.input
    xml_path = args.path
    print "Counting records in input XML..."
    input_count = count_elements_with_path(input_filename, xml_path)

    output_filename = args.output
    print "Counting documents in output JSON..."
    output_count = line_count(output_filename)

    if input_count == output_count:
        print "SUCCESS: Input and output have the same number of entries (%d)" % input_count
    else:
        print "FAIL: Input has %d entries, output has %d entries" % (input_count, output_count)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('input', help = "Path of input XML file")
    parser.add_argument('output', help = "Path of output JSON file")
    parser.add_argument('--path', help = "Path of entries in XML file", default = "ReleaseSet/ClinVarSet/ReferenceClinVarAssertion")
    args = parser.parse_args()
    run_test(args)
