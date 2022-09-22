import os
import re

table = open("Filtered Records.csv", "r")
results = open("Results.txt", "w")
table.readline()
scRNA = []
# qstat = os.popen("qstat | grep -c wanluliu").read().rstrip("\n")
# qstat_list = {}
# if qstat != "0":
#     tasks_in_progess = os.popen("qstat | grep wanluliu | awk '{print $3\" \"$5}'").read().replace("r", "Running").replace("qw", "Queuing").rstrip("\n")
#     results.write("The counting tasks of " + qstat + " samples have not been finished yet.\n")
#     results.write(tasks_in_progess + "\n\n")
#     for line in tasks_in_progess.split("\n"):
#         qstat_list[line.split(" ")[0]] = line.split(" ")[1]
results.write("Sample\tPaper\tResult\n")
os.chdir("scripts_outputs_errors")
for line in table:
    items = line.rstrip("\n").split("\t")
    sample = items[0]
    paper = items[3]
    # if sample in qstat_list.keys():
    #     results.write(sample + "\t" + paper + "\t" + qstat_list[sample].replace("Running", "The task is still running").replace("Queuing", "The task is still queuing") + "\n")
    #     continue
    result = ""
    output_found = False
    for file in os.listdir("./"):
        if len(file) > len(sample) + 5 and (file[ : len(sample) + 5] == sample + ".sh.o" or file[ : len(sample) + 5] == sample + ".sh.e"):
            output_found = True
            output = open(file, "r")
            for line in output:
                line = line.strip()
                if line == "Pipestance completed successfully!":
                    result = line
                    break
                if line == r"We expect that at least 50% of the reads exceed the minimum length.":
                    result = r"At least 50% of the reads should exceed the minimum length"
                if line.find("failed to download ") != -1:
                    result = "Failed to download"
                    break
                if line == "ERROR: The current CPU does not support sse4.2 instructions, and is no":
                    result = "The current CPU does not support sse4.2 instructions"
                    break
                if line.startswith("In the input data, an extremely low rate of correct barcodes was observed for this chemistry") or line.startswith("An extremely low rate of correct barcodes was observed for all the candidate chemistry choices for the input:"):
                    result = "Extremely low rate of correct barcodes was observed for a chemistry"
                    break
                if len(line) > 39:
                    if line[-39 : ] == "is larger than maximum allowed: skipped":
                        result = "The sra to prefetch exceeds the maximum size"
            if len(line) > 30:
                if line[-24 : ] == "Downloading via HTTPS..." and result == "":
                    result = "Prefetching sra"
                if line[-24 : ] == "Downloading via https...":
                    result = "Prefetching sra"
                if (line.find("has 0 unresolved dependencies") != -1 or line.find("was downloaded successfully") != -1) and result == "":
                    result = "Dumping fastq files"
                if line[20 : 35] == "Fasterq dumping":
                    result = "Fasterq dumping"
                if line[20 : 29] == "[runtime]":
                    result = "Counting in cellranger"
                if re.match(r"Written \d+ spots for .+\.sra", line):
                    result = "Downloaded successfully"
    if output_found:
        if result == "":
            result = "No result detected in the output file"
        results.write(sample + "\t" + paper + "\t" + result + "\n")
    else:
        results.write(sample + "\t" + paper + "\t" + "The task has not been run yet" + "\n")
table.close()
results.close()