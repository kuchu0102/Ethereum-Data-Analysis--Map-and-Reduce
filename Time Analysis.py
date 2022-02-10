from mrjob.job import MRJob
import re
import time

class course(MRJob):

    def mapper(self, _, line):
        #the field has been splitted with the ','
        fields=line.split(",")
        try:
            if (len(fields)==7):
                #Date has been coverted to string and the epoch time is converted to normal date
                date = int(fields[6])
                #date has been extracted , taking years and months
                year = time.strftime('%Y-%B', time.gmtime(date))
                yield (year,1)
        except:
            pass
#reducer to sum the number of transaction per month
    def reducer(self, year, counts):
        yield (year,sum(counts))

    def combiner(self, year, counts):
        yield (year,sum(counts))


if __name__ == '__main__':
     course.run()
