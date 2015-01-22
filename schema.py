schema = {
    # the path of the root element for every entry
    'path': "ReleaseSet/ClinVarSet/ReferenceClinVarAssertion",
    # the path of the elements to split the XML file by (for parallel processing)
    'split_path': "ReleaseSet/ClinVarSet",
    'fields': {
        'rcvaccession': {
            #  the path of the element from which the field is sourced
            'path': 'ClinVarAccession',
            # the attribute where content will be extracted from
            # if unspecified, content is extracted from the element body
            'attribute': 'Acc',
            # the type of the field
            'type': str
        },
        'rcvaccession_version': {
            'path': 'ClinVarAccession',
            'attribute': 'Version',
            'type': int
        },
        'rcvaccession_full': {
            # the fields the value of this field depends on
            'dependencies': ['rcvaccession', 'rcvaccession_version'],
            # function to transform the field value at the end
            # first argument is the value of the field before transformation
            # second argument is a list of the values of the dependencies
            'transform': lambda _, deps: "%s.%d" % (deps[0], deps[1]),
            'type': str
        },
        'title': {
            'absolute_path': 'ReleaseSet/ClinVarSet/Title',
            'type': str
        },
        'preferred_name': {
            'path': 'MeasureSet/Measure/Name/ElementValue',
            'type': str
        },
        'hgvs': {
            'path': 'MeasureSet/Measure/AttributeSet/Attribute',
            'type': [str],
            # only consider elements for which the filter function returns True
            'filter': lambda el: 'HGVS' in el.attrib.get('Type')
        },
        'clinical_significance': {
            'path': 'ClinicalSignificance/Description',
            'type': str
        },
        'entrez_gene_id': {
            'path': 'MeasureSet/Measure/MeasureRelationship/XRef',
            'attribute': 'ID',
            'type': [str],
            'filter': lambda el: el.attrib.get('DB') == 'Gene'
        },
        'rs_id': {
            'path': 'MeasureSet/Measure/XRef',
            'attribute:': 'ID',
            'type': [str],
            'filter': lambda el: el.attrib.get('DB') == 'Gene',
            'transform': lambda values, _: map(lambda x: x + "rs", values)
        },
        'uuid': {
            'dependencies': ['rcvaccession_full'],
            'transform': lambda _, deps: deps[0],
            'type': str
        }
    }
}
