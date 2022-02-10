# Ethereum Data Analysis using Hadoop's MapReduce

The project was intended to extract meaningful information from the ethereum data which has around 1 billion rows for period 2015-2019 and takes space of around 4GB.
Hadoop clusters was used to extract and process the data. MapReduce famous libarry Mrjob has been used to write the python code.
The code aimed to find the ethereum transactions over time, top 10 transactions and Gas Price(A general reference for approximate transaction fees on the Ethereum blockchain, gas price refers to the amount of ETH (in a small unit called gwei) that must be paid to miners for processing transactions on the network).

Results : 

![image](https://user-images.githubusercontent.com/61591442/153461357-8e85036f-1d2e-435a-b44e-3ccd02d4de0c.png)


![image](https://user-images.githubusercontent.com/61591442/153461589-6de4030f-6dce-4918-be3b-d4c3f7e67629.png)


![image](https://user-images.githubusercontent.com/61591442/153461678-5c1e7599-59f8-49e1-81fe-20dda8ade4a1.png)

There was also comparison between writing code in python and spark and to conclude how the spark is extremly fast than mrjob at processing huge chunks of data. More info can be found in the file 'Coursework-Kushagra Gupta- 190649351' .
