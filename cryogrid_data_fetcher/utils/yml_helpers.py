from .. import logger


def validate_yaml_file(filename_yaml: str, filename_schema: str):
    """
    Validate a yaml file against a schema file. 

    Parameters
    ----------
    filename_yaml : str
        The path to the yaml file.
    filename_schema : str
        The path to the schema file.
    """
    import yamale

    schema = yamale.make_schema(filename_schema)
    data = yamale.make_data(filename_yaml)
    yamale.validate(schema, data)


def make_template_from_schema(filename_schema: str, filename_template=None)->str:
    """
    Create a template yaml file from a schema file. 
    
    If (e.g. value) is provided use that as an example for the template.

    Parameters
    ----------
    filename_schema : str
        The path to the schema file.
    filename_template : str, optional
        The path to the template file. If None, the template will be returned as a string. 
        Default is None.
    
    Returns
    -------
    str
        The template yaml file name OR the template yaml file as a string.
    """
    import re

    with open(filename_schema, 'r') as f:
        schema = f.read()

    patterns = dict(
        schema_comment = re.compile(r'^#!.*'),
        empty_or_comment = re.compile(r'^\s*$|^\s*#.*'),
        key_value = re.compile(  # built with the help of chatGPT
            r'^(?P<key> *[A-Za-z0-9_]+) *:'  # key
            r' *(?P<value>.*?)'              # value
            r'(?: *'                         # optional comment
            r'(?P<comment># .*?)'            # comment
            r'(?:\s*\(e\.g\.\s*'             # optional example
            r'(?P<example>.*)'               # example
            r'\))?)?$'),                     # end of example    
    )

    template = []
    for i, line in enumerate(schema.split('\n')):
        if re.match(patterns['schema_comment'], line):
            logger.log(1, f'Skipping line {i:03d} schema comment ({line})')
            continue
        elif re.match(patterns['empty_or_comment'], line):
            logger.log(1, f'Keeping line {i:03d} empty or comment ({line})')
            template += line, 
        else:
            logger.log(1, f'Processing line {i:03d} key-value pair ({line})')
            m = re.match(patterns['key_value'], line).groupdict()
            m = {k: add_quotes_if_curly_braces(v) if v is not None else '' for k, v in m.items()}
            template += "{key}: {example}  {comment}".format(**m),
            
    template = '\n'.join(template)
    if filename_template is None:
        return template
    else:
        with open(filename_template, 'w') as f:
            f.write(template)
        return filename_template
    

def add_quotes_if_curly_braces(s: str)->str:
    """
    Add quotes to a string if it contains curly braces. 
    """
    if isinstance(s, (int, float)):
        return s
    elif isinstance(s, bool):
        return s
    elif s is None:
        return s
    elif s == "":
        return s
    else:
        if "{" in s and "}" in s:
            return f"'{s}'"
        return s