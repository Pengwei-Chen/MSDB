import os

# Environment settings
table = "Subset.csv"
cellranger = "/u/home/w/wanluliu/biosoft/cellranger-6.1.2/bin/cellranger"
transcriptome = "/u/home/w/wanluliu/wanluliu/refData/refdata-gex-GRCh38-2020-A"
delete_after_count = True
qsub = False

# Process table
print('Processing table "' + table + '"...')
file = open(table, "r", encoding = "utf-8")
header = file.readline().replace("\ufeff", "").rstrip("\n").split("\t")
run = header.index("Comment[ENA_RUN]")
assay = "scRNA-seq"
layout = header.index("Comment[LIBRARY_LAYOUT]")
sample = header.index("Comment[ENA_SAMPLE]")
paper = "2020_Alivernini S"
read_1 = header.index("Comment[FASTQ_URI]")
read_2 = read_1 + 2
index = read_2 + 2
scRNA = {}
for line in file:
    items = line.split("\t")
    if assay == "scRNA-seq" and items[layout] == "SINGLE":
        if items[sample] in scRNA:
            scRNA[items[sample]].append([items[sample], items[run], assay, paper.replace(" ", "_"), items[layout], items[read_1], items[read_2], items[index]])
        else:
            scRNA[items[sample]] = [[items[sample], items[run], assay, paper.replace(" ", "_"), items[layout], items[read_1], items[read_2], items[index]]]
file.close()
file = open("Filtered Records.csv", "w")
file.write("Sample\tRun\tAssay.Type\tPaper\tLayout\tRead_1\tRead_2\tIndex\n")
count = 0
for sample in scRNA.values():
    for run in sample:
        count += 1
        string = ""
        for item in run:
            string += item + "\t"
        string = string[:-1] + "\n"
        file.write(string)
file.close()
file = open(str(count) + ' scRNA-seq runs detected', "w")
file.close()

print()
print()

# Make directories
paper = 3
papers = set()
for sample in scRNA.values():
    for run in sample:
        papers.add(run[paper])
papers = list(papers)
string = ""
for paper in papers:
    string += paper + ", "
string = string[:-2]
print("Making directories", string, "...")
directories = os.listdir()
for paper in papers:
    if directories.count(paper) == 0:
        command = "mkdir " + paper
        print(command)
        print(os.popen(command).read().rstrip("\n"))
if directories.count("scripts_outputs_errors") == 0:
        command = "mkdir scripts_outputs_errors"
        print(command)
        print(os.popen(command).read().rstrip("\n"))
for paper in papers:
    os.chdir(paper)
    directories = os.listdir()
    if directories.count("01_sra") == 0:
        command = "mkdir 01_sra"
        print(command)
        print(os.popen(command).read().rstrip("\n"))
    if directories.count("01_fastq") == 0:
        command = "mkdir 01_fastq"
        print(command)
        print(os.popen(command).read().rstrip("\n"))
    os.chdir("../")

print()
print()

# Pipeline
sample = 0
name = 1
paper = 3
layout = 4
read_1 = 5
read_2 = 6
index = 7
for runs_of_a_sample in scRNA.values():
    count = 0
    command = "#!/bin/sh\n#$ -cwd\n#$ -o ./\n#$ -V\n#$ -S /bin/bash\n#$ -l h_data=105G,h_rt=24:00:00\n#$ -e ./\n# -pe shared 16\n\n"
    command += "cd ../\n"
    for run in runs_of_a_sample:
        count += 1
        command += "\n"

        # Download
        command += "cd " + run[paper] + "/01_fastq" + "\n"
        command += "wget " + run[read_1] + "\n"
        command += "wget " + run[read_2] + "\n"
        command += "wget " + run[index] + "\n"
        command += "cd ../../\n"

        # Rename
        print("Renaming fastq files...")
        suffix = ".fastq.gz"
        command += "cd " + run[paper] + "/01_fastq" + "\n"
        command += "\tmv " + run[index].split("/")[-1] + " " + run[sample] + "_S1_L00" + str(count) + "_I1_001" + suffix + "\n"
        command += "\tmv " + run[read_1].split("/")[-1] + " " + run[sample] + "_S1_L00" + str(count) + "_R1_001" + suffix + "\n"
        command += "\tmv " + run[read_2].split("/")[-1] + " " + run[sample] + "_S1_L00" + str(count) + "_R2_001" + suffix + "\n"
        command += "cd ../../\n"

    # Cellranger count
    command += "\n"
    print("Counting", run[sample] + "...")
    command += "cd " + run[paper] + "\n"
    command += cellranger + " count --id=02_output_" + run[sample] +" --transcriptome=" + transcriptome + " --fastqs=./01_fastq --sample=" + run[sample] + " --no-bam --localcores=16 --localmem=100\n"
    command += "cd ../\n"

    # Delete raw data if delete_after_count is set
    if delete_after_count:
        print("Deleting sra and fastq files...")
        command += "rm " + run[paper] + "/01_fastq/" + run[sample] + "_S1_L00*.fast*" + "\n"
    os.chdir("scripts_outputs_errors")
    file = open(run[sample] + ".sh", "w")
    file.write(command)
    file.close()
    if qsub:
        print("qsub " + run[sample] + ".sh")
        print(os.popen("qsub " + run[sample] + ".sh").read().rstrip("\n"))
    os.chdir("../")
    print()