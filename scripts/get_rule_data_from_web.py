# Purpose: Get rule data and combine them into a table 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/28/2023 (htu) - initial coding
#   

import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_rule_data_from_web(
        i_std: str = "sdtm", 
        i_url: str = "https://www.pinnacle21.com/validation-rules",
        o_dir: str = "./data/source"
        
    ):
    url = f"{i_url}/{i_std}"
    ofn_xlx = f"{o_dir}/xlsx/rule-{i_std}.xlsx"
    ofn_csv = f"{o_dir}/csvs/rule-{i_std}.csv"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', attrs={'id': 'rules'})
    headers = [th.text.strip() for th in table.find_all('th')]
    print(f"Headers: {headers}")

    # Set up Selenium WebDriver with headless Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)

    # Extract the JavaScript array containing the data
    js_data = driver.execute_script("return rulesData")

    # Define the DataFrame columns based on the HTML table headers
    # headers = ["Rule ID", "Publisher ID", "Message (English)", "Message (Chinese)", "Description (English)", "Description (Chinese)",
    #        "Domains", "FDA", "PMDA 2010.2", "PMDA 2211.0", "NMPA", "FDA Severity", "PMDA Severity", "3.1.2", "3.1.3", "3.2", "3.3", "Notes"]

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(js_data, columns=headers)

    # Write the DataFrame to an Excel file
    df.to_excel(ofn_xlx, index=False)
    df.to_csv(ofn_csv, index=False)

    # Close the driver
    driver.quit()
    df_len = len(df)
    print(f"Total records for {i_std.upper()}: {df_len}\n Describe: {df.describe()}") 
    return df 


# Test cases
if __name__ == "__main__":
    df_sdtm = get_rule_data_from_web(i_std="sdtm")
    df_send = get_rule_data_from_web(i_std="send")
    df_adam = get_rule_data_from_web(i_std="adam")
    df_define = get_rule_data_from_web(i_std="define-xml")
