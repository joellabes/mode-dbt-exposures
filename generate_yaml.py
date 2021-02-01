import json;
import re;
import os;
import yaml;

table_ref_lookup = {};

with open("/Users/joel/Projects/ep-dbt-project/target/manifest.json") as file:
    data = json.load(file)
    for node in data['nodes']:
        if not data['nodes'][node]['resource_type'] in ('test', 'analysis'):
            table_and_schema = f"{data['nodes'][node]['schema']}.{data['nodes'][node]['alias']}".replace("dev_jlabes.", "analytics.");
            table_ref_lookup[table_and_schema] = data['nodes'][node]['name'];

base_path = "/Users/joel/Projects/ep-mode-analytics/Mode/educationperfect/spaces/Business Summaries";

for filename in os.scandir(base_path):
    if filename.is_dir():
        print()
        print (filename.name)
        depends_ons = []

        metadata = filename.name.split(".");
        report_name = metadata[0];
        unique_id = metadata[1]

        exposure = {
            "version": 2,
            "exposures": [
                {
                    "name": report_name,
                    "description": "",
                    "type": "dashboard",
                    "url": "https://app.mode.com/educationperfect/reports/" + unique_id,
                    "depends_on": [],
                    "owner": {
                        "name": "Data Team",
                        "email": "joel.labes@educationperfect.com"
                    }
                }
            ]

        }

        for files in os.scandir(filename.path):
            if (files.name == "settings.yml"):
                with open(files.path) as settings:
                    exposure['exposures'][0]["description"] = yaml.safe_load(settings.read())['report_description'];
            if (files.path.endswith(".sql")):
                with open(files.path) as file:
                    text = file.read()
                    matches = re.findall("(?:from|join)\s+([\w\d]+\.[\w\d]+)", text);
                    for match in matches:
                        value = table_ref_lookup.get(match, match)
                        depends_ons.append(f"ref('{value}')")
        exposure['exposures'][0]['depends_on'] = depends_ons
        print(exposure);

        with open(f"/Users/joel/Desktop/{report_name}.yaml", 'w') as target:
            yaml.dump(exposure, target);

with open("/Users/joel/Projects/ep-mode-analytics/Mode/educationperfect/spaces/Business Summaries/2021 Invoicing Worm.502eaa9fa4ff/settings.yml") as definition:
    yml = yaml.safe_load(definition);
    print(yml['report_description']);

