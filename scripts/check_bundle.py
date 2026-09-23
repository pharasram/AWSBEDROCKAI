"""Validate merged bundle YAML against the installed CLI's official JSON schema.

This is an offline schema check, not authenticated workspace validation.
Usage: databricks bundle schema > work-schema.json
       python scripts/check_bundle.py work-schema.json
"""
import copy
import json
import sys
from pathlib import Path
import jsonschema
import regex
import yaml

ROOT = Path(__file__).resolve().parents[1]


def merged_bundle():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for pattern in bundle["include"]:
        for path in ROOT.glob(pattern):
            resource_file = yaml.safe_load(path.read_text())
            for kind, definitions in resource_file["resources"].items():
                current = bundle.setdefault("resources", {}).setdefault(kind, {})
                if set(current) & set(definitions):
                    raise ValueError("Duplicate resource definition")
                current.update(definitions)
    return bundle


def main():
    schema = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
    bundle = merged_bundle()
    # CLI patterns use Unicode property escapes (\p{L}) unsupported by Python re.
    # Use a Unicode-capable regex engine without weakening schema constraints.
    def unicode_pattern(validator, pattern, instance, schema):
        if isinstance(instance, str) and regex.search(pattern, instance) is None:
            yield jsonschema.ValidationError(f"{instance!r} does not match {pattern!r}")

    base = jsonschema.validators.validator_for(schema)
    validator = jsonschema.validators.extend(base, {"pattern": unicode_pattern})
    validator(schema).validate(copy.deepcopy(bundle))
    print("PASS: merged bundle conforms to Databricks CLI schema")


if __name__ == "__main__":
    main()
