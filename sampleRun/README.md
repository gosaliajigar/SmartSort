## Usage Commands ##


### Generate Data ###
hadoop jar target/SmartSort.jar edu.itu.csc.generate.SmartSortGenerateData 
	-input  [comma separated absolute path to input seed files]
	-output $(date +%Y%m%d%H%M%S) 
	-delimiter ,


### Pre Validation (MapReduce) ###
hadoop jar target/SmartSort.jar edu.itu.csc.validators.SmartSortPreValidator 
	-input [absolute path to input file] 
	-output $(date +%Y%m%d%H%M%S) 
	-total 9 
	-delimiter ,


### Sort (MapReduce) ###
hadoop jar target/SmartSort.jar edu.itu.csc.driver.SmartSortDriver 
	-primary 0 
	-secondary 1 
	-total 9 
	-input [absolute path to input file] 
	-output $(date +%Y%m%d%H%M%S) 
	-delimiter , 
	-porder asc 
	-sorder asc


### Post Validation (MapReduce) ###
hadoop jar target/SmartSort.jar edu.itu.csc.validators.SmartSortPostValidator 
	-input [absolute path to reducer output files i.e. part-r-*]
	-output $(date +%Y%m%d%H%M%S) 
	-delimiter , 
	-total 9
	-porder asc 
	-sorder asc
