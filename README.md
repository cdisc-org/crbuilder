
# CORE Rule Builder

RuleBuilder is a Python package for processing CDISC conformance rules. It 
provides a `RuleBuilder` class that can be used to read rule definitions from 
an Excel file or a YAML file, and then process those rules by applying them to 
datasets.

## Installation

To install RuleBuilder, run the following command:
```python
pip install rulebuilder
```


## Usage

The `RuleBuilder` class can be used to process conformance rules in three main 
steps: 
* initialize the rule builder, 
* build a single rule, or process all rules.

### Initialize

The `initialize` command is used to create a new instance of the `RuleBuilder` 
class. It takes the following command-line options:

```sql
--r_standard: the name of the standard for which the rule definitions are being processed (default: 'SDTM_V2_0')
--r_dir: the directory where the rule definitions are stored (default: None)
--i_fn: the name of the file that contains the rule definitions for the specified standard (default: 'SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml')
--core_base_url: the URL for the CORE base JSON schema (default: 'https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json')
--creator_url: the URL for the rule editor (default: 'https://rule-editor.cdisc.org/.auth/me')
```

To initialize a new instance of the RuleBuilder class, run the following command:

```Python 
rulebuilder initialize
```


You can specify any of the command-line options described above to customize the 
initialization of the `RuleBuilder` instance.

### Build a Single Rule

The `build_rule` command is used to build a single rule. It takes the following 
command-line options:

```java
--rule_id: the ID of the rule to build (default: None)
```


Replace `<rule_id>` with the ID of the rule you want to build.

### Process All Rules

The `process` command is used to process all rules. It takes the following 
command-line options:

```sql
--r_ids: a list of rule IDs to include (default: None)
--s_version: a list of versions to include (default: [])
--s_class: a list of classes to include (default: [])
--s_domain: a list of domains to include (default: [])
--wrt2log: a flag indicating whether to write output to a log file (default: 1)
--pub2db: a flag indicating whether to publish rules to a database (default: 0)
--get_db_rule: a flag indicating whether to get rules from a database (default: 1)
--db_name: the name of the database to use (default: None)
--ct_name: the name of the container to use (default: 'core_rules_dev')
```

To process all rules with the `RuleBuilder` class, run the following command:

```
rulebuilder process
```

You can specify any of the command-line options described above to customize the 
processing of the rules.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.


