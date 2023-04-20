
# CORE Rule Builder

RuleBuilder is a Python package for processing CDISC conformance rules. It 
provides a `RuleBuilder` class that can be used to read rule definitions from 
an Excel file or a YAML file, and then process those rules by applying them to 
datasets. Here is a list of steps that it performs:

* Read rule definition. Here is a list of available rule definitions:
  * FDA_VR1_6: FDA Validator Rules v1.6 December 2022_0
  * SDTM_V2_0: SDTM and SDTMIG Conformance Rules_v2.0
  * SEND_V4_0: SEND Conformance Rules v4.0
  * ADAM_V4_0: ADaM Conformance Rules v4.0
* Get rule statistics if `get_db_rule` or `pub2db` is enabled.
* Back up current rule(s) 
* Get existing rule if the rule exists in the database or in local rule folder
* Check rule citation against rule definition 
* Add missing citations to the rule
* Write the rule to target folders in JSON and YAML formats
* Publish the rule to the database if `pub2db` is enabled

## Installation

To install `RuleBuilder` class, run the following command:

```python
pip install crbuilder
```

## Usage

The `RuleBuilder` class can be used to process conformance rules with many commands and options. 
It can perform the following tasks: 

* initialize the rule builder, 
* get statistics for documents in a container
* build a single rule, or process a list of rules or all rules.

Here is the command line usage:

```python
Usage: rulebuilder.py [OPTIONS] COMMAND [ARGS]...
```

This produce a list of available commands:

```java
Options:
  --help  Show this message and exit.

Commands:
  build-rule
  get-doc-statistics
  initialize
  process
```

### Initialize

The `initialize` command is used to create a new instance of the `RuleBuilder` 
class. It takes the following command-line options:

```java
--r_standard: the name of the standard for which the rule definitions are being processed (default: 'SDTM_V2_0')
--r_dir: the directory where the rule definitions are stored (default: None)
--i_fn: the name of the file that contains the rule definitions for the specified standard (default: 'SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml')
--core_base_url: the URL for the CORE base JSON schema (default: 'https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json')
--creator_url: the URL for the rule editor (default: 'https://rule-editor.cdisc.org/.auth/me')
```

To initialize a new instance of the `RuleBuilder` class, run the following command:

```java 
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

```java
--r_standard: The name of the standard for which the rule definitions are being processed (default='SDTM_V2_0')
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

```java
python rulebuilder.py process --r_ids all  --pub2db 1
```

You can specify any of the command-line options described above to customize the 
processing of the rules. Here are a few examples:

``` java
python rulebuilder.py process --r_ids "CG0001,    CG0002, cg0015" --pub2db 1
python rulebuilder.py process --s_domain "AE,DM" --pub2db 1
python rulebuilder.py process --s_version "3.4" --pub2db 1
python rulebuilder.py process --s_class "EVT"  --pub2db 1
```

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.

## History


### Version 0.4.3

* Fixed rule type overwritten issue
* Enabled comment preserving process
* Added component diff

### Version 0.4.2

* Added logic to update duplicated rule documents
* Output all the files to the job folder
* Fixed 4 issues related to Scope and Authorities

### Version 0.4.1

* Enhanced logging messages
* Fixed additional issues related SDTM standards
* Added standard switches and record filtering

### Version 0.4.0

* Added capability of processing FDA conformance rules
* Added log level for creating log details
* Fixed 3 issues related to SDTM rule processing

### Version 0.3.1

* Moved from my personal repo [core-rule-builder](https://github.com/htu/core-rule-builder) to CDISC repo [crbuilder](https://github.com/cdisc-org/crbuilder)
* Added command-line options
* Added README
* Added backup as a default behavior while getting rule definition from database

### Version 0.3.0

* Added logging capability to produce a log file per rule
* Added report capability for all the rules processed
* Added reading from and publishing to the database
* Added backup capability
* Added de-duplication capability

### Version 0.2.0

* Added multiple rule processing capability
* Added error handling and message processing
* Added processing status report
* Fixed some rule bugs  

### Version 0.1.0

* Set up process framework
* Read rules from a local rule folder
* Produced single rule and pass rule validation
