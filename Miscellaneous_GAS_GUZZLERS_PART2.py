from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import statistics

class gas_guzzlers(MRJob):

    sector_table = {}

    def mapper_join_init(self):
        f = ['0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444','0xfa52274dd61e1643d2205169732f29114bc240b3','0x7727e5113d1d161373623e5f49fd568b4f543a9e',
        '0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef','0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8'] # Top 5 ranks
        for i in range(len(f)):
            key = f[i]
            value = None
            self.sector_table[key] = key

    def mapper_repl_join(self, _, line):
        # try:
        fields = line.split(',')
        if len(fields) == 7:
            address = fields[2]
            if address in self.sector_table:
                date = int(fields[6])
                year = time.strftime('%Y-%m-%d', time.gmtime(date))
                gas = float(int(fields[4]))
                key = (self.sector_table[address], year)  # The join has been done and the address has been mapped and sent as the key
                yield (key, gas)
        # except:
        #     pass


    def mapper_length(self, key, gas):
        yield(key, gas)


    def reducer_sum(self, key, gases):
        total = statistics.mean(gases)    #Mean has been found out for the gas
        yield (key, total)

    def steps(self):
          return [MRStep(mapper_init=self.mapper_join_init,
                          mapper=self.mapper_repl_join),
                  MRStep(mapper=self.mapper_length,
                          reducer=self.reducer_sum)]

if __name__ == '__main__':
  gas_guzzlers.run()
