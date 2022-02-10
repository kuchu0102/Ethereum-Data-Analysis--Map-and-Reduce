import pyspark
import re

#function to get the contracts dataset
def contracts(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False
        
#function to get the transaction dataset
def transactions(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        return False


sc = pyspark.SparkContext()

transactionFileData = sc.textFile("/data/ethereum/transactions")

filteredTransactions = transactionFileData.filter(transactions)

mappedTransactions = filteredTransactions.map(lambda l: (l.split(",")[2], int(l.split(",")[3])))

reducedTransactions = mappedTransactions.reduceByKey(lambda a, b: a + b)

contactFileData = sc.textFile("/data/ethereum/contracts")

filteredContacts = contactFileData.filter(contracts)

mappedContracts = filteredContacts.map(lambda l: (l.split(",")[0],None))

joinTable = reducedTransactions.join(mappedContracts) #joining the tables

top10 = joinTable.takeOrdered(10, key=lambda x: -x[1][0]) #Ordering for top 10
for record in top10:
    print("{} : {}".format(record[0], record[1][0]))
