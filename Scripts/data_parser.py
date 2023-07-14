import time
import os
from bs4 import BeautifulSoup
import datetime
import pandas

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#TODO

VIEWTIME = 2
date_format = "%m/%d/%Y"

CHARGES = None
DETAILS = None
OBLIGATIONS = None
CASEDETAILS = None
HEARINGS = None
EVENTS = None

FAILED_QUERIES = None

def normalize_column_names(column_names):
    normalized_names = []
    for name in column_names:
        # remove spaces, colons, and slashes
        cleaned_name = name.replace(' ', '').replace(':', '').replace('/', '')
        # convert to camel case
        words = cleaned_name.split('_')
        normalized_words = [words[0]] + [w.title() for w in words[1:]]
        normalized_name = ''.join(normalized_words)
        normalized_names.append(normalized_name)
    return normalized_names

def findIndex(driver, dates):
    try:
        no_data = driver.find_element(By.XPATH, "//td[text()='No Data']")
        #Wait for the data to load
        WebDriverWait(driver, VIEWTIME).until(EC.staleness_of(no_data))
    except Exception as e:
        print(e)

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    citations_table = soup.select_one("#tblCitations_ContentPlaceHolder1_VehicleSearch1_hidUniqueNameMaker")

    trs = citations_table.find_all("tr")

    for i, tr in enumerate(trs):
        # Skip the first row since it's the header
        if i == 0:
            continue

        tds = tr.find_all("td")
        violation_date = tds[5].text.strip()
        violation_date = datetime.datetime.strptime(violation_date, date_format)

        # Check if the violation date falls within the desired range
        if dates["start"] <= violation_date and violation_date <= dates["end"]:
            return i

def records_exist(driver):
    #First scrape the data on the charges page
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    charges_table = soup.select_one("#ctl00_ContentPlaceHolder1_CitationInfo1_ChargesView1_rgCharges_ctl00")

    print("Scraping data...")

    tds = charges_table.find_all("td")

    if "No records" in tds[0].text:
        return False
    else:
        return True

def parse_data(driver):
    #First scrape the data on the charges page
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    charges_table = soup.select_one("#ctl00_ContentPlaceHolder1_CitationInfo1_ChargesView1_rgCharges_ctl00")

    print("Scraping data...")
    
    #print("Charges table:")

    #Labels
    chargesLabels = charges_table.select("thead tr th a")
    chargesLabelTexts = []
    for charge in chargesLabels:
        chargesLabelTexts.append(charge.get_text(strip=True))
    #print(chargesLabelTexts)

    #Clean the labels
    chargesLabelTexts = normalize_column_names(chargesLabelTexts)

    #Data
    chargesData = charges_table.select("tbody tr")
    dataText = []
    for row in range(1, len(chargesData)): #start at 1 because the first tr in the html table contains no data
        data = chargesData[row].select("td")
        dataText.append([])
        for td in range(len(data)):
            dataText[row-1].append(data[td].text.strip())
    #print(dataText)

    #Convert the labels as columns and the data as rows in pandas
    pandas_charges_table = pandas.DataFrame(dataText, columns=chargesLabelTexts)
    #print(pandas_charges_table)

    #print("Citation Details:")

    def get_text_content_from_column(ID, childNode, soup, index=0):
        data_div = soup.select(f'#{ID}')[index] #The website is poorly built and they have elements with multiple ids; the second one (first index) is the correct one
        spans = data_div.select(childNode)
        return [span.get_text(strip=True) for span in spans]

    try:

        labels = [*get_text_content_from_column('LabelColumn1', childNode="div, a", soup=soup, index=1), *get_text_content_from_column('LabelColumn2', childNode="div, a", soup=soup, index=1)]
        #Clean/Normalize the Labels b/c these will be the Column Names for a Table
        labels = normalize_column_names(labels)
        #print(labels)

        data = [*get_text_content_from_column('DataColumn1', childNode="span", soup=soup, index=1), *get_text_content_from_column('DataColumn2', childNode="span, a", soup=soup, index=1)]
        print(data)

        # create Pandas dataframe
        row = pandas.DataFrame([data], columns=labels)

        DateScraped = pandas.DataFrame({"DateScraped": [datetime.date.today()]}) #Add date the data was scraped to be its own data frame and join it
        row = pandas.concat([DateScraped, row], axis=1)
        print(row)

        #Add some key information to the the charges table that will be used for matching and merging later one
        table_information = pandas.DataFrame({
            "DateScraped": len(pandas_charges_table)*[datetime.date.today()],
            "CitationNumber": len(pandas_charges_table)*[row.at[0,"CitationNumber"]]
        })

        pandas_charges_table = pandas.concat([table_information, pandas_charges_table], axis=1)
        print(pandas_charges_table)


        #Save both tables to the RAM for this iteration
        receive_table(table=row, name="details")
        receive_table(table=pandas_charges_table, name="charges")

        #After parsing the data on the available page, move to the obligations table to parse that data
        parse_obligations(driver, row.at[0,"CitationNumber"])

        if len(row.at[0, "CaseNumber"]) > 0:
            
            print("Parsing cases data...")
            try:
                caseNumberLink = driver.find_element(By.ID, "ContentPlaceHolder1_CitationInfo1_lbCaseNumber")
                caseNumberLink.click()
                case_table = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_divCaseDetails"))
                    )
            except Exception as e:
                print(f"Unable to parse case data: {e}")
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            case_details = soup.select_one("#ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_divCaseDetails")

            
            labels = [*get_text_content_from_column('LabelColumn1', childNode="div", soup=case_details), *get_text_content_from_column('LabelColumn2', childNode="div, a", soup=case_details)]
            # Clean/Normalize the Labels b/c these will be the Column Names for a Table
            labels = normalize_column_names(labels)

            data = [*get_text_content_from_column('DataColumn1', childNode="span", soup=case_details), *get_text_content_from_column('DataColumn2', childNode="span, a", soup=case_details)]

            # create Pandas dataframe
            caserow = pandas.DataFrame([data], columns=labels)

            information = pandas.DataFrame({"DateScraped": [datetime.date.today()], "CitationNumber": [row.at[0,"CitationNumber"]]}) #Add date the data was scraped and citation number
            caserow = pandas.concat([information, caserow], axis=1)

            #print(caserow)
            receive_table(table=caserow, name="case")

            #Scrape the hearings and events tables as well

            #Hearings
            
            try:
                #The fifth to last link is the link that leads to the hearings table
                # links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLink")))
                # links[-5].click()

                # links = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLink")))
                # links = driver.find_elements(By.CSS_SELECTOR, ".rtsLink")
                # links[-5].click()

                print("Parsing Hearings Data...")
                try:
                    links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLink")))
                    links[-5].click()
                except:
                    print("Unable to click link.")

                hearings_table = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_CaseHearingView1_rgHearings_ctl00"))
                )

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                hearings_table = soup.select_one("#ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_CaseHearingView1_rgHearings_ctl00")

                if "No records to display." in hearings_table.get_text():
                    print("There are no records in the hearings table")
                else:
                    headings = hearings_table.select("thead tr th")
                    headings_labels = []
                    for i in range(0, len(headings)):
                        headings_labels.append(headings[i].text.strip())

                    rows = hearings_table.select("tbody tr")

                    data = []
                    for i in range(1, len(rows)):
                        tr = []
                        currentRowData = rows[i].select("td")
                        for j in range(0, len(headings_labels)):
                            text_data = currentRowData[j].text.strip()
                            tr.append(text_data)
                        data.append(tr)

                    hearings = pandas.DataFrame(data, columns=normalize_column_names(headings_labels))

                    table_information = pandas.DataFrame({
                        "DateScraped": len(hearings)*[datetime.date.today()],
                        "CitationNumber": len(hearings)*[row.at[0,"CitationNumber"]]
                    })

                    hearings = pandas.concat([table_information, hearings], axis=1)
                    receive_table(table=hearings, name="hearings")

                    #print(hearings)
                    time.sleep(VIEWTIME)
            except Exception as e:
                print("Unable to parse hearings data")
                print(e)

            #Events
            try:
                #elements have gone stale, reestabilsh DOM
                #The fourth to last link is the events link
                # links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLink")))
                # links[-4].click()

                # link = WebDriverWait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLink:nth-last-of-type(4)")))
                # link.click()

                # link = WebDriverWait(driver, 10).until(
                #     EC.visibility_of_element_located((By.CSS_SELECTOR, ".rtsLink:nth-last-of-type(4)"))
                # )
                # link = WebDriverWait(driver, 10).until(
                #     EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLink:nth-last-of-type(4)"))
                # )
                # link.click()

                # links = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLink")))
                # links = driver.find_elements(By.CSS_SELECTOR, ".rtsLink")
                # links[-4].click()

                print("Parsing Events Data...")
                links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLink")))
                links[-4].click()

                events_table = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_CaseEventList1_rgCaseEvents_ctl00"))
                )

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                events_table = soup.select_one("#ctl00_ContentPlaceHolder1_CaseInfo1_rpbCaseInfo_i0_CaseEventList1_rgCaseEvents_ctl00")

                if "No records to display." in events_table.get_text():
                    print("There are no records in the hearings table")
                else:
                    headings = events_table.select("thead tr th")
                    headings_labels = []
                    for i in range(0, len(headings)):
                        headings_labels.append(headings[i].text.strip())

                    rows = events_table.select("tbody tr")

                    data = []
                    for i in range(1, len(rows)):
                        tr = []
                        currentRowData = rows[i].select("td")
                        for j in range(0, len(headings_labels)):
                            text_data = currentRowData[j].text.strip()
                            tr.append(text_data)
                        data.append(tr)

                    events = pandas.DataFrame(data, columns=normalize_column_names(headings_labels))

                    table_information = pandas.DataFrame({
                        "DateScraped": len(events)*[datetime.date.today()],
                        "CitationNumber": len(events)*[row.at[0,"CitationNumber"]]
                    })

                    events = pandas.concat([table_information, events], axis=1)
                    receive_table(table=events, name="events")

                    #print(events)
                    time.sleep(VIEWTIME)

            except Exception as e:
                print("Unable to click the link")   
                print(e)                


    except Exception as e:
        print("Failed to parse data...")
        print(e)

def parse_obligations(driver, citationNumber):
    print("Parsing Obligations")
    time.sleep(VIEWTIME)

    try:
        #Click on the obligations table link
        #The last link is the link that leads to the obligations table
        #links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLI.rtsLast")))
        # links = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLink")))
        # links = driver.find_elements(By.CSS_SELECTOR, ".rtsLink")
        # links[-3].click()
        # links = WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, ".rtsLink")))
        # links[-3].click()
        links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".rtsLI.rtsLast")))
        links[-1].click()

        # Wait for the last element of the ".rtsLink" class to be clickable
        #last_rtsLink_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".rtsLI.rtsLast:last-of-type")))

        # Click on the last element of the ".rtsLink" class
        #last_rtsLink_element.click()

        #Load the data
        print("Loading Data from Obligations Table...")
        obligations = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_CitationInfo1_ObligationList1_rgObls"))
                    )
        
        #Parse the data
        print("Parsing Data...")
        time.sleep(VIEWTIME)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        obligations = soup.select_one("#ctl00_ContentPlaceHolder1_CitationInfo1_ObligationList1_rgObls_ctl00")

        headings = obligations.select("thead tr th")
        headings_labels = []
        for i in range(0, len(headings)-1):
            headings_labels.append(headings[i].text.strip())

        rows = obligations.select("tbody tr")

        data = []
        for i in range(1, len(rows)):
            tr = []
            currentRowData = rows[i].select("td")
            for j in range(0, len(headings_labels)):
                text_data = currentRowData[j].text.strip()
                if j == len(headings_labels)-1:
                    print("Expanding remarks")
                    try:
                        # Find the button element by its ID
                        button = soup.select_one('#btnExpand')
                        # Extract the onclick attribute
                        onclick_attr = button.get('onclick')
                        # Find the text within the onclick function
                        text_data = onclick_attr.split('btnExpand_onclick(')[1].rstrip(');').replace('"','')
                    except Exception as e:
                        print("Unable to expand remarks")
                tr.append(text_data)
            data.append(tr)

        obligations = pandas.DataFrame(data, columns=normalize_column_names(headings_labels))

        table_information = pandas.DataFrame({
            "DateScraped": len(obligations)*[datetime.date.today()],
            "CitationNumber": len(obligations)*[citationNumber]
        })

        obligations = pandas.concat([table_information, obligations], axis=1)

        print(obligations)

        #Save data to RAM
        receive_table(table=obligations, name="obligations")
    except Exception as e:
        print("Unable to click link")
        print(e)

def receive_table(table=None, name=""):
    global CHARGES
    global DETAILS
    global OBLIGATIONS
    global CASEDETAILS
    global HEARINGS
    global EVENTS

    global FAILED_QUERIES

    print("Saving data to RAM...")

    #Takes a pandas table as an argument and stores it in the RAM
    if table is None:
        #end of program
        print("End of program.") 

        if FAILED_QUERIES is not None:
            FAILED_QUERIES_FRAME = pandas.DataFrame(FAILED_QUERIES)
            print(FAILED_QUERIES_FRAME)
            with open("../Data/failed_queries.csv", "a") as f:
                f.write('\n')
                FAILED_QUERIES_FRAME.to_csv(f, index=False, mode='a', header=False)

        print("Saving data to files...")

        if CHARGES is not None:
            print(CHARGES)
            print(f"Unique citations in CHARGES table: {CHARGES['CitationNumber'].nunique()}")
            CHARGES.to_csv("../Data/charges.csv", index=False, mode='a', header=False)
        if DETAILS is not None:
            print(DETAILS)
            print(f"Unique citations in DETAILS table: {DETAILS['CitationNumber'].nunique()}")
            DETAILS.to_csv("../Data/details.csv", index=False, mode='a', header=False)
        if OBLIGATIONS is not None:
            print(OBLIGATIONS)
            print(f"Unique citations in OBLIGATIONS table: {OBLIGATIONS['CitationNumber'].nunique()}")
            OBLIGATIONS.to_csv("../Data/obligations.csv", index=False, mode='a', header=False)
        if CASEDETAILS is not None:
            print(CASEDETAILS)
            print(f"Unique citations in CASE DETAILS table: {CASEDETAILS['CitationNumber'].nunique()}")
            CASEDETAILS.to_csv("../Data/case_details.csv", index=False, mode='a', header=False)
        if HEARINGS is not None:
            print(HEARINGS)
            print(f"Unique citations in the HEARINGS table: {HEARINGS['CitationNumber'].nunique()}")
            HEARINGS.to_csv("../Data/hearings.csv", index=False, mode='a', header=False)
        if EVENTS is not None:
            print(EVENTS)
            print(f"Unique citations in the EVENTS table: {EVENTS['CitationNumber'].nunique()}")
            EVENTS.to_csv("../Data/events.csv", index=False, mode='a', header=False)

    else:
        if "charges" in name:
            if CHARGES is None:
                CHARGES = table
                #print(CHARGES)
            else:
                CHARGES = pandas.concat([CHARGES, table], ignore_index=True) #Reindex the rows upon concatenation
                #print(CHARGES)
        elif "details" in name:
            if DETAILS is None:
                DETAILS = table
                #print(DETAILS)
            else:
                DETAILS = pandas.concat([DETAILS, table], ignore_index=True) #Reindex the rows upon concatenation
                #print(DETAILS)
        elif "obligations" in name:
            if OBLIGATIONS is None:
                OBLIGATIONS = table
                #print(OBLIGATIONS)
            else:
                previous_length = len(OBLIGATIONS)
                OBLIGATIONS = pandas.concat([OBLIGATIONS, table], ignore_index=True) #Reindex the rows upon concatenation
                #print(f"{previous_length} + {len(table)} = {len(OBLIGATIONS)}")
                #print(OBLIGATIONS)
        elif "case" in name:
            if CASEDETAILS is None:
                CASEDETAILS = table
                #print(CASEDETAILS)
            else:
                CASEDETAILS = pandas.concat([CASEDETAILS, table], ignore_index=True)
                #print(CASEDETAILS)
        elif "hearings" in name:
            if HEARINGS is None:
                HEARINGS = table
                #print(HEARINGS)
            else:
                HEARINGS = pandas.concat([HEARINGS, table], ignore_index=True)
                #print(HEARINGS)
        elif "events" in name:
            if EVENTS is None:
                EVENTS = table
                #print(EVENTS)
            else:
                EVENTS = pandas.concat([EVENTS, table], ignore_index=True)
                #print(EVENTS)


def failed_query_tracker(citation, plate=None, violationDate=None):
    global FAILED_QUERIES
    print("Failed Citation:")
    print(f"Citation: {citation} \nPlate: {plate}\nViolation Date: {violationDate}")
    print()
    if FAILED_QUERIES is None:
        FAILED_QUERIES = {
            "DateScraped": [datetime.date.today()],
            "CitationNumber": [citation],
            "Vehicle": [plate],
            "ViolationDate": [violationDate]
        }
    else:
        FAILED_QUERIES["DateScraped"].append(datetime.date.today())
        FAILED_QUERIES["CitationNumber"].append(citation)
        FAILED_QUERIES["Vehicle"].append(plate)
        FAILED_QUERIES["ViolationDate"].append(violationDate)