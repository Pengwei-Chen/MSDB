import os

bash = True

table = open("Rerun List.csv")
for line in table:
    items = line.rstrip("\n").split("\t")
    sample = items[0]
    paper = items[1]

    # Remove cellranger output folders and .mro files (presented during the counting process)
    for file in os.listdir("./" + paper):
        if file == "02_output_" + sample:
            command = "rm -r " + paper + "/" + file
            print(command)
            print(os.popen(command).read().strip())
        if file == "__02_output_" + sample + ".mro":
            command = "rm " + paper + "/" + file
            print(command)
            print(os.popen(command).read().strip())

    # Remove task outputs and errors
    for file in os.listdir("./scripts_outputs_errors"):
        if file[ : len(sample) + 5] == sample + ".sh.o" or file[ : len(sample) + 5] == sample + ".sh.e":
            command = "rm " + "./scripts_outputs_errors/" + file
            print(command)
            print(os.popen(command).read().strip())

    # Resubmit the job
    if bash:
        os.chdir("./scripts_outputs_errors")
        command = "bash " + sample + ".sh" + " &>> " + sample + ".sh.output"
        print(command)
        print(os.popen(command).read().strip())
        os.chdir("../")
table.close()