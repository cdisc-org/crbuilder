import io
import json
import sys
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap


def convert_json_to_yaml(json_data, output_file=None, write_to_file=False):
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)

    data = json.loads(json_data)

    yaml_data = CommentedMap()

    for key, value in data.items():
        if key.startswith("# "):
            continue

        comment_key = f"# {key}"
        yaml_data[key] = value

        if comment_key in data:
            comment = data[comment_key]
            yaml_data.ca.items[key] = [
                None, [f"{comment}", None, None, None]]

    if write_to_file and output_file:
        with open(output_file, 'w') as outfile:
            yaml.dump(yaml_data, outfile)
    else:
        return yaml_data


if __name__ == "__main__":
    json_data = '''
    {
      "# name": "This is a comment for the name field",
      "name": "John",
      "# age": "This is a comment for the age field",
      "age": 30,
      "# city": "This is a comment for the city field",
      "city": "New York"
    }
    '''
    yaml = YAML()

    convert_json_to_yaml(
        json_data, output_file='output.yml', write_to_file=True)

    yaml_data = convert_json_to_yaml(json_data)

    text_stream = io.StringIO()

    yaml.dump(yaml_data, text_stream)

    text_stream.seek(0)

    for line in text_stream:
        print(line, end='')
