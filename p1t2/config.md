# Requirements
You also need to provide a config file which describes configurations or addition step you did to run the **following 3 tasks on a single Spark node **

- Use the program in Part 1 Task 2 to take “enwiki_small.xml” as input to generate the graph.
- Use the program in Part 1 Task 3 to take the graph you just generated and output a rank list of the articles in the dataset.
- (Optional) Use the stream emitter you wrote in Part 2 Task 2 to emit the rank list output in the previous step to a local directory while using the stream receiver you wrote in Part 2 Task 1 to dynamically read the files and generate the output mentioned in Part 2 Task 1.

# Cofiguration
The input file is "enwiki_small.xml". 
We used a single node cluster with 1GB memory for driver and 5GB memory for executor for Part 1 Task2. Also, we used a standard cluster with 2 workers for Part 1 Task 3.

The input file is "enwiki_small.xml". 

**Note: the p1t2.ipynb file generates a CSV file with an automatically assigned name under the foler named as "q2-small". Our CSV file is manually renamed to p1t2.csv afterwards.**