# Purpose: Get rule data from various sources
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   05/09/2023 (htu) - initial coding extracted from scripts/rule_html
#

from bs4 import BeautifulSoup

def write_df2html(df_tbl, fp_html, tbl_title:str=None): 
    # convert the dataframe to an HTML table
    html_table = df_tbl.to_html(render_links=True, escape=False)

    # create a BeautifulSoup object
    soup = BeautifulSoup(html_table, 'html.parser')

    if not soup.html:
        soup.append(soup.new_tag('html'))
    if not soup.head:
        soup.html.append(soup.new_tag('head'))    # add some additional styling to the table
    table_style = soup.new_tag('style')
    table_style.string = 'table {border-collapse: collapse;} th, td {border: 1px solid black; padding: 5px;}'

    # append the styling to the head of the HTML document
    soup.head.append(table_style)

    # print the final HTML table
    # print(soup.prettify())

    # create a file object for writing
    
    with open(fp_html, 'w') as f:
        # write the prettified HTML code to the file
        f.write(soup.prettify())
    print(f"  Written to {fp_html}.")
