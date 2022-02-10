from mrjob.job import MRJob

class top_10(MRJob):

  def mapper(self, _, line):
    # try:
    fields=line.split('\t')    #the field has been splitted with the '\t'
    address = fields[0]
    address = address[1:-1]    #To remove the '""' from the field value
    value = int(fields[1])
    pair = (address,value)
    yield(None,pair)
    # except:
    #     pass

  def reducer(self, _,values):
      sorted_values = sorted(values, reverse=True,key=lambda l:l[1])  #lambda function to sort the values
      i=0
      for value in sorted_values:
          if i<10:
              yield(i,"{} - {}".format(value[0],value[1])) #yielding the values
              i+=1
          else:
            break
        # sorted_values = sorted_values[:10]
        # for value in sorted_values:
        #     address = value[0]
        #     value = value[1]
        #     yield(address,value)
if __name__ == '__main__':
    top_10.run()
