# Purpose: Rule Builder Class
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/07/2023 (htu) - initial coding
# Examples:
# python rulebuilder.py process --r_ids none  --pub2db 1
# python rulebuilder.py process --r_ids all  --pub2db 1
# python rulebuilder.py process --r_ids "CG0001,    CG0002, cg0015" --pub2db 1
# python rulebuilder.py process --s_domain "AE,DM" --pub2db 1
# python rulebuilder.py process --s_version "3.4" --pub2db 1
# python rulebuilder.py process --s_class "EVT"  --pub2db 1
# python rulebuilder.py process --r_standard FDA_VR1_6 --r_ids CT2001 --ct_name editor_rules_dev
# python rulebuilder.py process --r_standard FDA_VR1_6 --r_ids CT2001 --ct_name editor_rules_dev --pub2db 1
# python rulebuilder.py process --r_standard SDTM_V2_0 --r_ids "CG0155,CG0156" --ct_name editor_rules_dev --pub2db

import click
from rulebuilder.rbuilder import RuleBuilder

@click.group()
def cli():
    pass


@cli.command()
@click.option('--r_standard', default='SDTM_V2_0', help='The name of the standard for which the rule definitions are being processed.')
@click.option('--r_dir', default=None, help='The directory where the rule definitions are stored.')
@click.option('--i_fn', default='SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml', help='The name of the file that contains the rule definitions for the specified standard.')
@click.option('--core_base_url', default='https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json', help='The URL for the CORE base JSON schema.')
@click.option('--creator_url', default='https://rule-editor.cdisc.org/.auth/me', help='The URL for the rule editor.')
def initialize(r_standard, r_dir, i_fn, core_base_url, creator_url):
    if r_standard is None:
        print("No rule definition standard is provided.")
        print(" . Nothing will be initialized. ")
        return 
    return RuleBuilder(r_standard=r_standard, r_dir=r_dir, i_fn=i_fn,
                     core_base_url=core_base_url, creator_url=creator_url)


@cli.command()
@click.option('--rule_id', default=None, help='The ID of the rule to build.')
def build_rule(rule_id):
    rb = RuleBuilder()
    rule_json = rb.build_a_rule(rule_id)
    print(rule_json)


@cli.command()
@click.option('--db_name', type=str, default=None, help='Name of database to query.')
@click.option('--ct_name', type=str, default=None, help='Name of container to query.')
def get_doc_statistics(db_name, ct_name):
    rb = RuleBuilder()
    rb.get_doc_stats(db_name=db_name, ct_name=ct_name)


@cli.command()
@click.option('--r_standard', default='SDTM_V2_0', help='The name of the standard for which the rule definitions are being processed.')
@click.option('--r_ids', default=None, help='A list of rule IDs to include.')
@click.option('--s_version', default=None, help='A list of versions to include.')
@click.option('--s_class', default=None, help='A list of classes to include.')
@click.option('--s_domain', default=None, help='A list of domains to include.')
@click.option('--wrt2log', default=1, help='A flag indicating whether to write output to a log file.')
@click.option('--pub2db', default=0, help='A flag indicating whether to publish rules to a database.')
@click.option('--get_db_rule', default=1, help='A flag indicating whether to get rules from a database.')
@click.option('--db_name', default=None, help='The name of the database to use.')
@click.option('--ct_name', default='core_rules_dev', help='The name of the container to use.')
def process(r_standard:str=None, r_ids:str=None, s_version:str=None, 
            s_class:str=None, s_domain:str=None, 
            wrt2log:int=1, pub2db:int=1, 
            get_db_rule:int=0, db_name:str=None, ct_name:str=None):
    rb = RuleBuilder(r_standard=r_standard)
    if r_ids is None:
        if s_version is None and s_class is None and s_domain is None: 
            v_ids = ["XXXX"]
        else: 
            v_ids = []
    elif r_ids.upper() == "ALL":
        v_ids = []
    else: 
        v_ids = [s.strip().upper() for s in r_ids.split(',')]
    v_ves = [] if s_version is None else [s.strip().upper() for s in s_version.split(',')]
    v_cls = [] if s_class is None else [s.strip().upper() for s in s_class.split(',')]
    v_dos = [] if s_domain is None else [s.strip().upper() for s in s_domain.split(',')]
    # print(f"Version: {len(v_ves)}: {v_ves}")
    rb.process(r_standard=r_standard,
        r_ids=v_ids, s_version=v_ves, s_class=v_cls, s_domain=v_dos,
               wrt2log=wrt2log, pub2db=pub2db, get_db_rule=get_db_rule, db_name=db_name, ct_name=ct_name)


if __name__ == "__main__":
    cli()
