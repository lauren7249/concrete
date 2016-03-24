csvs = {}

year = '2015'
import requests
from os import listdir
import re
import pickle
from dateutil.parser import parse
folder = "/Users/lauren/Documents/accounting/personaltaxes{}/".format(year)
filenames = listdir(folder)
API_KEY='5oo5di5r6a29'
for filename in filenames:
    name = filename.split(".")[0]
    period = re.search('[0-9]{6}', name).group(0)
    if period in csvs and csvs[period] !='usage limit exceeded: pay for more at https://pdftables.com/pricing\n':
        continue
    opened_file = open(folder + filename,'rb')
    files = {}
    files[name] = (filename, opened_file)        
    response = requests.post("https://pdftables.com/api?key={}&format=csv".format(API_KEY), files=files)
    csv = response.content
    csvs[period] = csv
pickle.dump(csvs, open('taxes_csvs.pickle','wb'))

#csvs = pickle.load(open('taxes_csvs.pickle','rb'))

def clean_description(description):
    description = description.replace(",",'').strip()
    description = re.sub('[^A-Z\s]','', description.upper()).strip()
    if description.find('NYC TAXI') == 0 or description.find('NYCTAXI') == 0:
        description = 'NYC TAXI'
    if description.find('DROPBOX') == 0:
        description = 'DROPBOX'    
    if description.find('LYFT') == 0:
        description = 'LYFT'          
    if description.find('MTA') == 0:
        description = 'MTA'             
    if description.find('SEAMLSS') == 0:
        description = 'SEAMLESS'             
    if description.find('STARBUCKS') == 0:
        description = 'STARBUCKS'              
    return description

import xlsxwriter
workbook  = xlsxwriter.Workbook('/Users/lauren/Documents/accounting/personaltaxes{}_items.xlsx'.format(year))
worksheet = workbook.add_worksheet()
purchases = []
for period, csv in csvs.iteritems():
    month = int(period[4:])
    purchase_lines = re.findall('(?<=\n).+,,[0-9]+\.[0-9][0-9],{0,1}(?=\n)',csv)
    all_lines = csv.splitlines()
    last_debit_date = None
    #if line is in purchase lines , add it to the purchase 
    n_purch = len(purchase_lines)
    for i in xrange(0, n_purch):
        line = purchase_lines[i]
        try:
            _line = re.sub('^,','',line)
            _line = re.sub(',$','',_line)
            chunks = _line.split(",")
            debit_date = chunks[0]
            amount = float(chunks[-1])
            description = " ".join(chunks[1:-1])  
            description = re.sub('\s+',' ',description)             
            if last_debit_date and parse(debit_date) < parse(last_debit_date):
                print month
                print line
                break             
            purchase = {}
            purchase["amount"] = amount

            if description.find("DBT Purchase")>-1:
                if i+1<n_purch: #not the last line of purchases
                    #use the line before the next line of purchases
                    next_line_index = all_lines.index(purchase_lines[i+1]) - 1
                else:
                    #use the next line
                    next_line_index = all_lines.index(line) + 1
                next_line = all_lines[next_line_index]
                date_sre = re.search('[0-9]{1,2}/[0-9]{1,2}/[0-9]{1,2}', description)
                if date_sre:
                    purchase_date = date_sre.group(0)
                description = " ".join(next_line.split(","))
                description = re.sub('\s+',' ',description)  
            else:
                purchase_date = debit_date
            purchase_month = int(purchase_date.split("/")[0])
            if month == 1 and purchase_month == 12:
                continue
            if month == 12 and purchase_month == 1:
                continue
            purchase_date = "/".join(purchase_date.split("/")[:2]) + "/" + year
            purchases.append([purchase_date, clean_description(description), amount])
            last_debit_date = debit_date
        except Exception, e:
            print str(e)
            print month
            print line
            import pdb
            pdb.set_trace()

def writerow(worksheet, rownum, row):
    for i in xrange(0, len(row)):
        worksheet.write(rownum, i, row[i])

header = ["Date", "description","amount"]
rownum =0
writerow(worksheet, rownum, header)
for i in xrange(0, len(purchases)):
    writerow(worksheet, i+1, purchases[i])

workbook.close()

tot = 0.0
from collections import defaultdict
counts = defaultdict(lambda: 0)
amounts = defaultdict(lambda: 0.0)
for purchase in purchases:
    description = clean_description(description)    
    amount = purchase[-1]
    counts[description] += 1
    amounts[description] += amount
    tot+=amount
tot += (7.0 * 2125) + (5 * 2200)

import operator
print sorted(amounts.items(), key=operator.itemgetter(1))