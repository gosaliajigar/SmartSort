## About ##
As part of CSC 550 Big Data Course, SmartSort was developed to demonstrate Secondary Sort design pattern using MapReduce for final term project.


## Introduction ##
  - SmartSort is an implementation of Secondary Sort design pattern in MapReduce/Hadoop.
  - A secondary sort relates to sorting values associated with a key in the reduce phase. It is sometimes called as value-to-key conversion. 
  - The secondary sorting technique will enable us to sort the values (in ascending or descending order) passed to each reducer.


## Summary ##
SmartSort has 4 steps ...
  1. Generate Data : 
      - Reads individual field seed data files
      - Using Cartesian Product, randomly combines the seed data
      - Generates max 10G (i.e. 103 100MB files)
2. Pre Sort Validation (MapReduce) : 
      - Validates generated file for no. of fields using delimiter used in generate phase.
3. Sort (MapReduce) : 
      - Sorts data using secondary sort for 4 different combinations of natural key and natural value.
4. Post Sort Validation (MapReduce) :
      - Given the order of sorting used in sorting phase, it validates sorted data as per sorting order mentioned.

Here we are using input seed data to generate random delimiter separated data (10G) and sort it in 4 different combinations (asc-desc) of primary key(natural key) and secondary key(natural value).


## Workflow ##
![workflow](https://cloud.githubusercontent.com/assets/5839686/24533223/0ddab106-157b-11e7-97d0-384856e5277f.jpeg)


## Sort Combinations ##
![combinations](https://cloud.githubusercontent.com/assets/5839686/24533208/f649d102-157a-11e7-915d-11cbf6a640a8.png)


## Code Structure ##
<img width="907" alt="packageexplorer" src="https://cloud.githubusercontent.com/assets/5839686/24580175/5b1d1682-16b8-11e7-85e3-6c5effb7fe8b.png">


## Configuration ##
  - Single Node Configuration can be found under SmartSort/sampleRun/configurations/


## Input Seed Data ##
  - Seed Data can be found at SmartSort/resources/


## Dependencies ##
- commons-io-2.4.jar
- hadoop-core-1.2.1.jar


## Usage ##
SmartSort.jar is available under target/SmartSort.jar or can be generated from the code.

![usage](https://cloud.githubusercontent.com/assets/5839686/24533304/b73147b0-157b-11e7-8687-7019ba37273f.jpg)


## Sample Run ##
  - Sample Logs can be found under SmartSort/sampleRun/logs/


## Performance & Analysis ##
Screenshots for the period each step was running ...

| EXECUTION STEP | CPU STATS |
| ------------- | ------------- |
| Generate Data | ![generatedata](https://cloud.githubusercontent.com/assets/5839686/24533190/d9ed482c-157a-11e7-829e-dcdbfb2d542f.jpg) |
| Pre Validation | ![prevalidation](https://cloud.githubusercontent.com/assets/5839686/24533238/31cbebde-157b-11e7-8a58-b2f5a38a4479.jpg) |
| Sort | ![sort](https://cloud.githubusercontent.com/assets/5839686/24533250/4240ea1e-157b-11e7-98b8-94021c279b87.jpg) |
| Post Validation | ![postvalidation](https://cloud.githubusercontent.com/assets/5839686/24533259/5b3ff85c-157b-11e7-8da6-02c657287d2f.jpg) |


## DISCLAIMER ##
Any resemblance of seed data (first name, last name, street, state, city, zipcode, ipaddress, phone, credit-card, card-type) and random generated data from these seed data to real persons, living or dead, is purely coincidental.
