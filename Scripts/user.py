import time
import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import data_parser

PATH = "Driver/chromedriver.exe"
driver = webdriver.Chrome(PATH)

date_format = "%m/%d/%Y"

VIEWTIME = 2

#Filter in citation dates and match with specific case
#Select closest citation within 3 weeks (on or after violation date)
#Also record date the data is scraped
#Two separate tables for violations and obligations (set citation number as ID)

def match_date(date, query={"citation": None, "plate": None}):
    violations = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "tblCitations_ContentPlaceHolder1_VehicleSearch1_hidUniqueNameMaker"))
    )

    date = datetime.datetime.strptime(date, date_format)
    within_three_weeks = date + datetime.timedelta(weeks=3)

    print(f"Checking if violation date is within the range {date} and {within_three_weeks}")
    #print(date.strftime(date_format))

    #Find which row matches with the violation date so we can click on it
    data_parser.findIndex(driver, {"start": date, "end": within_three_weeks})
    matched_violation_index = data_parser.findIndex(driver, {"start": date, "end": within_three_weeks})

    if matched_violation_index != -1:
        citations_table = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "tblCitations_ContentPlaceHolder1_VehicleSearch1_hidUniqueNameMaker"))
                )
        citations = citations_table.find_elements(By.TAG_NAME, "tr")
        citation = citations[matched_violation_index]
        #citation.click()
        #print(matched_violation_index)

        fetch_citation(url="", citation=citation.text.split(" ")[0])

        # time.sleep(VIEWTIME)

        # data_parser.parse_data(driver)
    else:
        print("No matches found...")
        print()
        data_parser.failed_query_tracker(citation=query["citation"], plate=query["plate"], violationDate=date)

def evaluate_results(plate=False, date=None, query={"citation": None, "plate": None}):
    if plate:
        table = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "tblVehicleSearch_ContentPlaceHolder1_VehicleSearch1_hidUniqueNameMaker"))
                )
        try:
            WebDriverWait(driver, VIEWTIME).until(lambda driver: len(table.find_elements(By.TAG_NAME, "tr")) > 1)
            vehicles = table.find_elements(By.TAG_NAME, "tr")
            # print(len(vehicles))

            if len(vehicles) > 1:
                #The first row is the table header
                #If there is more than one row that means we have found a record
                #We can go ahead and go into the record
                try:
                    vehicles[1].click()
                except Exception as e:
                    # If the element becomes stale, find the table again and get the updated list of vehicles
                    table = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.ID, "tblVehicleSearch_ContentPlaceHolder1_VehicleSearch1_hidUniqueNameMaker"))
                            )
                    vehicles = table.find_elements(By.TAG_NAME, "tr")
                    vehicles[1].click()
                    #try:
                        #vehicles[1].click()
                    #except Exception as e:
                        #print("No results... Logging failed query...")
                        #data_parser.failed_query_tracker(citation=query["citation"], plate=query["plate"], violationDate=date)

                #Find the date that's three weeks or after the citation and click on it
                match_date(date, query=query)
        except Exception as e:
            print("Query failed...")
            data_parser.failed_query_tracker(citation=query["citation"], plate=query["plate"], violationDate=date)
            #print(e)
            print()
    else:
        data_parser.parse_data(driver)


def fetch_citation(url="", citation="", plate="", emptyRecords=False, date=None):
    if len(url) > 0:
        driver.get(url)
        driver.refresh()

    # Wait for the elements to be present on the page
    links = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "rtsLink"))
    )

    if not emptyRecords and isinstance(citation, str) and len(citation) > 5 and not ("{" in citation or "}" in citation or "nan" in plate):
        #First try the citation number
        #This is the most reliable because there's only one unique citation number

        # Find the 7th element with the class name "rtsLink"
        citation_link = links[6]

        #citation number link
        citation_link.click()

        #Search the citation number into the search input and submit the form
        search_bar = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_CitationInfo1_txtCitationNumber"))
        )
        print(f"Searching citation: {citation}")
        # if citation == "9Z0828346":
        #     print("BINGO\nBINGO\nBINGO\nBINGO\nBINGO\nBINGO\nBINGO\nBINGO\nBINGO\nBINGO\n")
        #     time.sleep(VIEWTIME)

        try:
            search_bar = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_CitationInfo1_txtCitationNumber"))
            )
            search_bar.clear()
        except Exception as e:
            print(f"Unable to clear the search bar: {e}")
        try:
            search_bar = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_CitationInfo1_txtCitationNumber"))
            )  
            search_bar.send_keys(citation)
        except Exception as e:
            print(f"Unable to send keys to the search bar: {e}")

        try:
            search_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_CitationInfo1_btnSearch"))
            )
            search_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_CitationInfo1_btnSearch"))
            )
            search_btn.click()
        except Exception:
            print("Unable to submit keys.")


        check_records(url, citation, plate, date)

    elif emptyRecords and isinstance(plate, str) and len(plate) > 3 and not ("{" in plate or "}" in plate or "nan" in plate):
        print("Citation number is invalid... we're trying the plate number...")

        #Next try the plate number; see if that yields any fruit
        plate_link = links[7]
        plate_link.click()

        time.sleep(VIEWTIME)

        #Search the Lincense plate number into the search input and submit the form
        search_bar = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_VehicleSearch1_txtVehiclePlate"))
        )
        print(f"Searching plate number: {plate}")

        try:
            search_bar = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_VehicleSearch1_txtVehiclePlate"))
            )
            search_bar.clear()
        except Exception as e:
            print(f"Unable to clear the search bar: {e}")    
        
        try:
            search_bar = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_VehicleSearch1_txtVehiclePlate"))
            )
            search_bar.send_keys(plate)
        except Exception as e:
            print(f"Unable to send keys to the the search bar: {e}")

        try:
            search_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_VehicleSearch1_btnSearch"))
            )
            search_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_VehicleSearch1_btnSearch"))
            )
            search_btn.click()
        except Exception:
            print("Unable to submit keys.")

        evaluate_results(plate=True, date=date, query={"citation": citation, "plate": plate})

    else:
        data_parser.failed_query_tracker(citation=citation, plate=plate)

def check_records(url, citation="", plate="", date=None):
    #After submitting the form check and see if there are any results
    no_data = driver.find_element(By.XPATH, "//*[@id='ctl00_ContentPlaceHolder1_CitationInfo1_ChargesView1_rgCharges_ctl00']/tbody/tr/td/div")
    try:
        #Wait for the data to load
        WebDriverWait(driver, 10).until(EC.staleness_of(no_data))
        if not data_parser.records_exist(driver):
            #Try searching the plate number
            #time.sleep(VIEWTIME)
            fetch_citation(url, citation=citation, plate=plate, emptyRecords=True, date=date)
            # if data_parser.queue_records(citation, All=True) == 0:
            #     fetch_citation(url, citation=citation, plate=plate, emptyRecords=True, date=date)
            # else:
            #     print(f"Queued records returns {data_parser.queue_records(citation, All=True)}")
            #     print("Proceeding on to the next citation...\n")
        else:
            print("Query returns records, evaluating results...")
            evaluate_results(plate=False, date=date, query={"citation": citation, "plate": None})
    except Exception as e:
        print("Reevaluating plate number because of timeout exception or there was no data.")
        #Search license plate if the wait for the citation query fails
        fetch_citation(url, citation=citation, plate=plate, emptyRecords=True, date=date)
        