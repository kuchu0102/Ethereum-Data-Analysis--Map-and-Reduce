from mrjob.job import MRJob
import time
import statistics
class gas_guzzlers(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')  #Line split
            if len(fields) == 7:
                date = int(fields[6])
                year = time.strftime('%Y-%m-%d', time.gmtime(date))  #epoch time change
                gas_price = float(int(fields[5]))
                yield (year,gas_price)
        except :
            pass

    def reducer(self, year, gas_price):
        yield (year,statistics.mean(gas_price))  #Average of gas price

    def combiner(self, year, gas_price):
        yield (year,statistics.mean(gas_price))  
if __name__ == '__main__':
  gas_guzzlers.run()
