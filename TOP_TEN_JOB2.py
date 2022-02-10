from mrjob.job import MRJob

class repartition_stock_join(MRJob):

    def mapper(self, _, line):
        #the field has been splitted with the ',' and '\t' for two different files
        fields = line.split(",")
        field = line.split("\t")
        try:
            if(len(field)==2):
                join_key = field[0]
                join_key = join_key[1:-1]     #To remove the '""' from the field value
                join_value = int(field[1])
                yield (join_key, (join_value,1)) #Join key from the first file


            if(len(fields)==5):
                join_key = fields[0]
                join_value = fields[3]
                yield (join_key, (join_value,2)) #Join key from the second file
        except:
            pass

    def reducer(self, address, values):

        val = None
        exist = None
        #Matching the values from the mappers.
        for value in values:
            if value[1] == 1:
                val = value[0]
            elif value[1] == 2:
                exist = value[0]

        if val is not None and exist is not None : #The condition to only take the value which were present in both the tables
            yield(address,val)

if __name__ == '__main__':
    repartition_stock_join.run()
