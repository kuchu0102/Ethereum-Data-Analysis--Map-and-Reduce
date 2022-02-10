from mrjob.job import MRJob
import re
import time

class course(MRJob):

    def mapper(self, _, line):
        #the field has been splitted with the ','
        fields=line.split(",")
        try:
            if (len(fields)==7):
                to_address = fields[2]
                value = int(fields[3])
                if value==0:
                    pass
                else:
                    yield (to_address,value)
        except:
            pass
#reducer to sum the number of values per address
    def reducer(self, key, counts):
        yield (key,sum(counts))
#combiner to sum the number of values per address
    def combiner(self, key, counts):
        yield (key,sum(counts))

if __name__ == '__main__':
     course.run()
