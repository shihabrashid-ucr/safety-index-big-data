The followings are included in the deliverables:
1) "Final Output Files" directory: contains all types of output files
2) "SafetyIndex" project directory: the project folder to calculate the safety index using Spark SQL in cluster mode
3) "Visualize" directory: contains all files related to visualize the output
4) "Final_Report_Group_16.pdf": The project report (final version)
5) "README.txt": this file
6) "Requirements.txt": mentions the required software and libraries for visualization

*************************************************************************************************************************
The scripts to run the project:

1) "SafetyIndex/script_cluster.sh":
	-the script should be in the "SafetyIndex/" project directory
	-takes an input folder, i.e., "./" that contains the datasets
	-saves the output in the same input ("./") directory
	-Command to run: "./script_cluster.sh ./"

2) "Visualization/python_script.sh":
	-the script should be in the "Visualization/" directory (in local machine)
	-the input files are the csv ones, should be copied from the cluster nodes to local one
	-generates the ouput files, i.e., heat map, statistical plots
	-it takes no input

3) "SafetyIndex/script.sh":
	-the script should be in the "SafetyIndex/" project directory
	-only useful if the createCrimeNYCDataset() is separately called
	-takes an input, same as "SafetyIndex/script_cluster.sh"
	-saves the "NYC_Crime_Dataset.csv" file in the input directory

************************************************************************************************************************
Instructions to run the project:

1) Copy the "SafetyIndex/" directory to the cluster
2) Copy the "Visualization/" directory in the local machine
3) Go to the "SafetyIndex/" directory, and run the following command to generate the output files of safety index:
	./script_cluster.sh ./
4) Copy the following output files from "SafetyIndex/" directory to the "Visualization/" directory in the local machine:
	- Safety_Index.csv
	- Statistics_Crime_type.csv
	- Statistics_Neighborhood.csv
5) Go to the "Visualization/" directory, and run the following command to generate image files for visualization:
	./pyhton_script.sh
