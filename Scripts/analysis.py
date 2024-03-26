#Merge changes into 1 table
#Fix warnings
#Fix analyzeChanges function to include only the dates that we're looking at
#Fix error on lab computer

import glob
import os
import pandas
import numpy as np
import datetime

def condense(dir = "Data", extension = "csv"):
    file_dir = dir
    file_paths = glob.glob(os.path.join(file_dir, f"*.{extension}"))

    for file in file_paths:
        #Remove remove-duplicates
        print(f"Removing duplicate entries for in {file}...")
        data = pandas.read_csv(file, on_bad_lines='skip')

        #data = data.drop(data.columns[[0, 1]], axis=1)

        # print(data)
        data = data.fillna('NA') #Fill the actual NaN values with string data so pandas can compare entries for equality
        data = data.drop_duplicates()
        data = data.reset_index(drop=True)
        data.to_csv(file, index=False)

#condense()

def clean(data):
    return float(data.strip().replace("$",""))

def obligations_summary(dir="Data", file="obligations.csv"):
    table = pandas.read_csv(f"{dir}/{file}")
    summary = table.groupby(["DateScraped", "CitationNumber"])

    differences = {
        "DateScraped": [],
        "CitationNumber": [],
        "TotalOriginalAmount": [],
        "TotalBalance": [],
        "BalanceDifference": []}

    for group, data in summary:
    #print(group)
    #print(data)
        for i in range(len(data)):
            OriginalAmounts = data["OriginalAmount"]
            original = np.zeros(len(OriginalAmounts))
            for j in range(len(OriginalAmounts)):
                original[j] = clean(OriginalAmounts.iloc[j,])
            Balances = data["Balance"]
            balances = np.zeros(len(Balances))
            for j in range(len(Balances)):
                balances[j] = clean(Balances.iloc[j,])

            original_total = np.sum(original)
            balance_total = np.sum(balances)
            difference = original_total-balance_total
            
            differences["DateScraped"].append(group[0])
            differences["CitationNumber"].append(group[1])
            differences["TotalOriginalAmount"].append(original_total)
            differences["TotalBalance"].append(balance_total)
            differences["BalanceDifference"].append(difference)
            
            #print(np.sum(original) - np.sum(balances))
    differences_data = pandas.DataFrame(differences)

    differences_data = differences_data.drop_duplicates().reset_index(drop=True)
    differences_data["BalanceChange"] = np.zeros(len(differences_data))
    differences_data["OriginalAmountChange"] = np.zeros(len(differences_data))
    differences_summary = differences_data.groupby("CitationNumber")
    for group, data in differences_summary:
        data_reindexed = pandas.DataFrame(data).reset_index(drop=True)
        amounts = data_reindexed["TotalOriginalAmount"].values
        amount0 = data_reindexed.at[0,"TotalOriginalAmount"]
        balance_differences = data_reindexed["BalanceDifference"].values
        balance0 = data_reindexed.at[0, "BalanceDifference"]
        for i in range(len(amounts)):
            if amount0 != amounts[i]:
                amount0 = amounts[i]
                data_reindexed.at[i,"OriginalAmountChange"] = True
            else:
                data_reindexed.at[i,"OriginalAmountChange"] = False
            if balance0 != balance_differences[i]:
                balance0 = balance_differences[i]
                data_reindexed.at[i,"BalanceChange"] = True
            else:
                data_reindexed.at[i,"BalanceChange"] = False
            
    #     print(differences_data.loc[data.index, "OriginalAmountChange"])
    #     print(data_reindexed["OriginalAmountChange"].values)
        
        differences_data.loc[data.index, "OriginalAmountChange"] = data_reindexed["OriginalAmountChange"].values
        differences_data.loc[data.index, "BalanceChange"] = data_reindexed["BalanceChange"].values

    # differences_data
    differences_data.sort_values(by=['CitationNumber', 'DateScraped'], ascending=False, inplace=True)
    differences_data = differences_data.reset_index(drop=True)

    return differences_data

def case_details_summary(dir="Data", file="case_details.csv"):
    case_details = pandas.read_csv(f"{dir}/{file}")

    case_data = case_details[["CitationNumber", "DateScraped", "CaseStatus", "DefenseAttorney"]].copy()
    case_data.loc[:,"CaseStatusChange"] = 0
    case_data.loc[:,"DefenseAttorneyChange"] = 0
    case_data_summary = case_data.groupby("CitationNumber")
    #Compare changes in attorney name and case_status
    for citation, data in case_data_summary:
        data_reindexed = pandas.DataFrame(data).reset_index(drop=True)
        casestatuses = data_reindexed["CaseStatus"].values
        casestatus0 = casestatuses[0]
        attorneys = data_reindexed["DefenseAttorney"].values
        attorney0 = attorneys[0]
        for i in range(len(data_reindexed)):
            if casestatuses[i] != casestatus0:
                casestatus0 = casestatuses[i]
                data_reindexed.at[i, "CaseStatusChange"] = True
            else:
                data_reindexed.at[i, "CaseStatusChange"] = False
            if attorneys[i] != attorney0 and not np.isnan(attorney0):
                attorney0 = attorneys[i]
                data_reindexed.at[i, "DefenseAttorneyChange"] = True
            else:
                data_reindexed.at[i, "DefenseAttorneyChange"] = False
        case_data.loc[data.index, "CaseStatusChange"] = data_reindexed["CaseStatusChange"].values
        case_data.loc[data.index, "DefenseAttorneyChange"] = data_reindexed["DefenseAttorneyChange"].values
        
    return case_data.sort_values(["CitationNumber", "DateScraped"], ascending=False).reset_index(drop=True)

def charges_summary(dir="Data", file="charges.csv"):
    charges = pandas.read_csv(f"{dir}/{file}")

    charges_data = charges[["CitationNumber", "DateScraped", "Dispo"]].copy()
    charges_data.loc[:,"DispoChange"] = 0
    charges_data_summary = charges_data.groupby("CitationNumber")
    for citation, data in charges_data_summary:
        data_reindexed = pandas.DataFrame(data).reset_index(drop=True)
        data_summary = data_reindexed.groupby("DateScraped")
        dispos_values = []
        for date, dispos in data_summary:
            dispos_reindexed = pandas.DataFrame(dispos).reset_index(drop=True)
            dispos_values.append(dispos_reindexed["Dispo"].values.tolist())
        #print(len(dispos_values)) -> should be the same for each group
        
        #Code to map f: {0,...,i}x{0,...,j} -> {0,...,nrow(data_reindexed)} ------------------------
        #Creates a dictionary that will allows us to find which entry (found by the index in the codomain)
        #    to store True/False in the data_reindexed array by using i and j
        i_values = []
        j_values = []

        for i in range(len(dispos_values)):
            for j in range(len(dispos_values[i])):
                i_values.append(i)
                j_values.append(j)

        index = {
            'i': i_values,
            'j': j_values
        }

        index['index'] = list(range(len(index['i'])))

        index = pandas.DataFrame(index)
        #-----------------------------------------------------------------------------
        
        dispo0 = dispos_values[0]
        for i in range(len(dispos_values)):
            for j in range(len(dispos_values[i])):
                data_index = index[(index["i"] == i) & (index["j"] == j)]["index"].values[0]
                if dispo0 == dispos_values[i]:
                    #Only count it false if each group of dispos are *exactly* the same. i.e. a change from ['DF'] to ['DF','AM'] will have two True values in both rows
                    if not (data_index > 0 and (data_reindexed.at[data_index-1, "DispoChange"] == True and (j > 0))):
                        data_reindexed.at[data_index, "DispoChange"] = False
                        
                else:
                    data_reindexed.at[data_index, "DispoChange"] = True
                    for k in range(len(dispos_values[i])):
                        data_reindexed.at[data_index+k, "DispoChange"] = True  
                    dispo0 = dispos_values[i]
                    
        charges_data.loc[data.index, "DispoChange"] = data_reindexed["DispoChange"].values
        
    return charges_data.sort_values(["CitationNumber", "DateScraped"], ascending=False).reset_index(drop=True)

def getChanges(o,c,ch): #pass in dfs from obligation_summary, case_details_summary, and charges_summary
    obligation_citations = o.groupby("CitationNumber")
    case_citations = c.groupby("CitationNumber")
    charge_citations = ch.groupby("CitationNumber")
    ChangeTotals = {
        "CitationNumber": [],
        "TotalChanges": []
    }
    #Some tables might contain new or different citations, so we will just summarize the changes after aggregation
    for citation, data in obligation_citations:
        ChangeTotals["CitationNumber"].append(str(citation))
        data = pandas.DataFrame(data)
        ChangeTotals["TotalChanges"].append(np.sum(data["BalanceChange"].values.astype(int)) + np.sum(data["OriginalAmountChange"].values.astype(int)))
    for citation, data in case_citations:
        ChangeTotals["CitationNumber"].append(str(citation))
        data = pandas.DataFrame(data)
        ChangeTotals["TotalChanges"].append(np.sum(data["CaseStatusChange"].values.astype(int)) + np.sum(data["DefenseAttorneyChange"].values.astype(int)))
    for citation, data in charge_citations:
        ChangeTotals["CitationNumber"].append(str(citation))
        data = pandas.DataFrame(data)
        ChangeTotals["TotalChanges"].append(np.sum(data["DispoChange"].values.astype(int)))
    print(ChangeTotals)
    ChangeTotals = pandas.DataFrame(ChangeTotals).groupby("CitationNumber").sum().reset_index()
    #Add a date column to the changes table so that we compare the changes across Change Totals over time
    ChangeTotals["DateAnalyzed"] = [datetime.date.today()]*len(ChangeTotals)
    return ChangeTotals.sort_values(["TotalChanges", "CitationNumber"], ascending=False)

def analyzeChanges(dir="Data", file="Total Changes.csv", summary_files=["Obligations Summary.csv", "Case Details Summary.csv", "Charges Summary.csv"], time_difference=1):
    data = pandas.read_csv(f"{dir}/{file}", on_bad_lines='skip')

    # print(data)
    data = data.fillna('NA') #Fill the actual NaN values with string data so pandas can compare entries for equality
    data = data.drop_duplicates()
    data = data.reset_index(drop=True)
    data.to_csv(f"{dir}/{file}", index=False)
    
    changes = pandas.read_csv(f"{dir}/{file}")
    dates = np.unique(changes["DateAnalyzed"].values)
    #If the threshold doesn't return any matches, then just compare the previous group
    current_date = datetime.datetime.strptime(changes["DateAnalyzed"].values[len(changes)-1], '%Y-%m-%d')
    threshold_date = current_date-datetime.timedelta(days=time_difference)

    current_changes = changes[(changes["DateAnalyzed"] == str(current_date).split(' ')[0])]
    past_changes = changes[(changes["DateAnalyzed"] == str(threshold_date).split(' ')[0])]

    differences = {
        "CitationNumber": [],
        "IncreasedChanges": [],
        "NewCitation": [],
        "DeletedCitation": []
    }

    if len(past_changes) == 0: #This means that our threshold was a bad choice
        #Then just compare the next most previous scraped data
        past_changes = changes[(changes["DateAnalyzed"] == dates[len(dates)-2])]

    for citation in current_changes["CitationNumber"]:
        row = current_changes[current_changes["CitationNumber"] == citation]
        #Because there should only be one entry for each unique citation, we can compare directly:
        
        past_changes_row = past_changes[past_changes["CitationNumber"] == citation]

        #If our new changes detect a newly added citation, then the current citation won't be in the past_changes table
        if len(past_changes_row) != 0:
            current_citation_changes = row["TotalChanges"].values[0]
            past_citation_changes = past_changes_row["TotalChanges"].values[0]

            difference = current_citation_changes != past_citation_changes

            if difference:
                differences["CitationNumber"].append(citation)
                differences["IncreasedChanges"].append(current_citation_changes-past_citation_changes)
                differences["NewCitation"].append(False)  
        else: #We will count it as a new change in the table
            differences["CitationNumber"].append(citation)
            differences["IncreasedChanges"].append(current_citation_changes)
            differences["NewCitation"].append(True)
            differences["DeletedCitation"].append(False)

    #Now we will checked for citations that were there previously but not anymore
    for citation in past_changes["CitationNumber"]:
        row = past_changes[past_changes["CitationNumber"] == citation]
        #See if there is a corresponding row in the most recent scrape
        row_exists = len(current_changes[current_changes["CitationNumber"] == citation]) != 0
        
        past_citation_changes = row["TotalChanges"].values[0]
        current_citation_changes = current_changes[current_changes["CitationNumber"] == citation]["TotalChanges"]
        
        if not row_exists:
            differences["CitationNumber"].append(citation)
            differences["IncreasedChanges"].append(-past_citation_changes)
            differences["NewCitation"].append(False)
            differences["DeletedCitation"].append(True)
        elif current_citation_changes.values[0] != past_citation_changes: #Only append false if the corresponding row has a change
            differences["DeletedCitation"].append(False)
        
    differences = pandas.DataFrame(differences)
    #print(differences)

    #Now we want to find where the changes took place:
    changed_data_array = []
    for i in range(len(summary_files)):
        summary_file = summary_files[i]
        summary_data = pandas.read_csv(f"{dir}/{summary_file}")

        changed_citations = summary_data[summary_data["CitationNumber"].isin(differences["CitationNumber"])]
        #print(changed_citations)

        changed_citations_grouped = changed_citations.groupby(["DateScraped", "CitationNumber"])
        groupAppended = False
        nextIteration = False
        changedData = pandas.DataFrame()
        
        #Reversing the order is important when we detect for past changes.
        #The loop first finds the row that is changed and then it finds the next group.
        #If it is reversed, we can see what the data has changed from
        
        for group, data in reversed(list(changed_citations_grouped)):
            if not changedData.empty:
                nextIteration = True
            changeCols = []
            for col in data:
                if "Change" in col:
                    changeCols.append(col)
            changed_data = data[(data[changeCols] == True).any(axis=1)]
            if len(changed_data) > 0:
                if changedData.empty:
                    changedData = changed_data.copy()
                else:
                    changedData = pandas.concat([changedData, changed_data])
                groupAppended = False
                nextIteration = True
            if nextIteration and not groupAppended and not changedData.empty:
                #Append the next group that doesn't have changes so we can see the changes that took place
                #We want to "Get to the bottom of it." i.e. We want to go back to the earliest date when there wasn't a change
                if len(data[(data[changeCols] == True).any(axis=1)]) <= 0:
                    changedData = pandas.concat([changedData, data])
                    groupAppended = True
                    nextIteration = False
        #print(changedData)
        if not changedData.empty:
            changed_data_array.append(changedData)
   
    return differences, changed_data_array

#analyzeChanges()

def merge_tables(o,c,ch):
    merged_table = (o.merge(c, on=["DateScraped", "CitationNumber"], how="left")).merge(ch, on=["DateScraped", "CitationNumber"], how="left").reset_index(drop=True)
    return merged_table

if __name__ == "__main__":
    condense()
    obligations = obligations_summary()
    case_details = case_details_summary()
    charges = charges_summary()
    totals = getChanges(obligations, case_details, charges)

    print(totals)