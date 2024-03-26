#1. Read the given sample data
#1b. Clean it if necessary
#2. Feed it in to our User.py script which will iterate over the data and run it through the website through Selenium
#3. Once the data has been run through the website and parsed through, send it back to this script where we will save the data

#Current Stale Element Errors:
#1) Unable to clear the search bar
#2) Unable to parse case data
#3) Unable to click links

import pandas
import time
import os
import glob
import schedule
from datetime import datetime, timedelta
import pytz

import user
import data_parser
import analysis
import email_data

class Citations:

    def __init__(self):
        file_dir = "Data/Citations"
        file_paths = glob.glob(os.path.join(file_dir, "*.csv"))

        self.data = file_paths
        self.url = "https://web.seattle.gov/smc/ECFPortal/Default.aspx"

    def get_data(self):
        return self.data

    def clean(self, data):
        data = str(data).replace("?", "").replace(".", "").split(" ")[0]
        return data

    def beginIteration(self):
        for i in range(len(self.data)):
            self.sample_data = pandas.read_csv(self.data[i])
            self.iterations = len(self.sample_data)

            print(self.sample_data)
            print(f"Number of possible iterations: {self.iterations}")

            for j in range(0, self.iterations):
                user.fetch_citation(self.url, 
                                    self.clean(self.sample_data.at[j, "citationnumber"]),
                                    self.clean(self.sample_data.at[j, "licenseplate"]),
                                    date=self.sample_data.at[j, "violationdate"])
                #time.sleep(1)
            data_parser.receive_table()
        analysis.condense()
        obligations = analysis.obligations_summary()
        case_details = analysis.case_details_summary()
        charges = analysis.charges_summary()
        totals = analysis.getChanges(obligations, case_details, charges)
        merged_summary = analysis.merge_tables(obligations, case_details, charges)

        print(obligations)
        print(case_details)
        print(charges)

        print("Here is the Grand Summary:\n")
        print(merged_summary)
        merged_summary.to_csv("Data/Grand Summary.csv", index=False)

        print("Here are the total changes since we started scraping of each citation:\n")
        print(totals)

        obligations.to_csv("Data/Obligations Summary.csv", index=False)
        case_details.to_csv("Data/Case Details Summary.csv", index=False)
        charges.to_csv("Data/Charges Summary.csv", index=False)
        
        #Append the changes to the total changes file so we can compare changes across time.
        #Of course, all the changes across time are already documented in the tables above, but this gives us quicker summary without looking at each entery of True/Fals values
        totals.to_csv("Data/Total Changes.csv", index=False, mode='a', header=False) #The index is the CitationNumber

        #Analyze the totals to see where the changes occured:
        differences, changes = analysis.analyzeChanges()
        email_dfs = []
        email_dfs.append(totals)
        if(len(differences) > 0):
            print("Here's where the changes occured: ")
            email_dfs.append(differences)
            for i in range(len(changes)):
                email_dfs.append(changes[i])
                print(changes[i])
        else:
            print("There were no new changes from the previous scrape.")

        print("Sending data to Sam...")
        email_data.email_new(email_dfs)

program = Citations()

def schedule_function():
    # Set timezone to U.S. Mountain Time
    tz = pytz.timezone("US/Mountain")

    # Calculate the next 6 a.m. and 6 p.m. in U.S. Mountain Time
    now = datetime.now(tz)
    next_6am = datetime(now.year, now.month, now.day, 6, 0, 0, tzinfo=tz)
    #next_6pm = datetime(now.year, now.month, now.day, 18, 0, 0, tzinfo=tz)

    # Schedule the function to run at 6 a.m.
    schedule.every().day.at(next_6am.strftime("%H:%M")).do(program.beginIteration)
    #schedule.every().day.at(next_6pm.strftime("%H:%M")).do(program.beginIteration)

if __name__ == "__main__":
    program.beginIteration()

    #Run the scraper every day at 6 a.m. and 6 p.m.
    schedule_function()

    while True:
        schedule.run_pending()
        time.sleep(1000)
