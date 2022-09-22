import os

# Environment settings
table = "New Data.txt"
prefetch = "/opt/shared/Biosoft/sratoolkit.3.0.0/bin/prefetch.3.0.0"
sra = "./"
if sra[-1:] != "/":
    sra += "/"
fastq_dump = "/opt/shared/Biosoft/sratoolkit.3.0.0/bin/fastq-dump"
fasterq_dump = "/opt/shared/Biosoft/sratoolkit.3.0.0/bin/fasterq-dump"
cellranger = "/opt/shared/Biosoft/cellranger-7.0.0/bin/cellranger-7.0.0"
transcriptome = "/mnt/volume3/cpw/refdata-gex-GRCh38-2020-A.tar.gz"
delete_after_count = True
fasterq = False
bash = False

# Process table
print('Processing table "' + table + '"...')
file = open(table, "r", encoding = "utf-8")
header = file.readline().replace("\ufeff", "").rstrip("\n").split("\t")
run = header.index("RunID")
assay = header.index("AssayType")
layout = header.index("LibraryLayout")
sample = header.index("SampleName")
paper = header.index("ProjectID")
scRNA = {}
for line in file:
    items = line.split("\t")
    if items[assay] == "scRNA-seq" and items[layout] == "PAIRED":
        if items[sample] in scRNA:
            scRNA[items[sample]].append([items[sample], items[run], items[assay], items[paper].replace(" ", "_"), items[layout]])
        else:
            scRNA[items[sample]] = [[items[sample], items[run], items[assay], items[paper].replace(" ", "_"), items[layout]]]
file.close()
file = open("Filtered Records.csv", "w")
file.write("Sample\tRun\tAssay.Type\tPaper\tLayout\n")
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
for runs_of_a_sample in scRNA.values():
    count = 0
    command = "#!/bin/sh\n#$ -cwd\n#$ -o ./\n#$ -V\n#$ -S /bin/bash\n#$ -l h_data=105G,h_rt=24:00:00\n#$ -e ./\n#$ -pe shared 16\n\n"
    command += "cd ../\n"
    for run in runs_of_a_sample:
        count += 1
        command += "\n"

        if not fasterq:
            # Prefetch
            print("Prefetching", run[name] + "...")
            command += prefetch + " -X 100G " + run[name] + "\n"

            # Move
            print("Moving", run[name] + "...")
            command += "mv " + sra + run[name] + "/" + run[name] + ".sra ./" + run[paper] + "/01_sra" + "\n"
            command += "rm -r "+ sra + run[name] + "\n"

            # Fastq-dump
            print("Dumping", run[name] + "...")
            command += "cd " + run[paper] + "\n"
            command += fastq_dump + " --outdir 01_fastq --gzip --split-files ./01_sra/" + run[name] + ".sra" + "\n"
            command += "cd ../\n"

            if delete_after_count:
                command += "rm " + run[paper] + "/01_sra/" + run[name] + ".sra" + "\n"

        else:
            # Fasterq-dump
            print("Fasterq dumping", run[name] + "...")
            command += "echo $(date +%F%n%T)' Fasterq dumping " + run[name] + "...'\n"
            command += "cd " + run[paper] + "\n"
            command += fasterq_dump + " --outdir 01_fastq --split-files -e 16 --include-technical " + run[name] + "\n"
            command += "cd ../\n"

        # Rename
        print("Renaming fastq files...")
        if fasterq:
            suffix = ".fastq"
        else:
            suffix = ".fastq.gz"
        command += "cd " + run[paper] + "/01_fastq" + "\n"
        command += "if [ -f ./" + run[name] + "_3" + suffix + " ]\n"
        command += "then\n"
        command += "\tif [ `head -n 1 ./" + run[name] + "_1" + suffix + " | awk -F 'length=' '{print $2}'` -lt `head -n 1 ./" + run[name] + "_3" + suffix + " | awk -F 'length=' '{print $2}'` ];\n"
        command += "\tthen\n"
        command += "\t\tmv " + run[name] + "_1" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_I1_001" + suffix + "\n"
        command += "\t\tmv " + run[name] + "_2" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R1_001" + suffix + "\n"
        command += "\t\tmv " + run[name] + "_3" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R2_001" + suffix + "\n"
        command += "\telse\n"
        command += "\t\tmv " + run[name] + "_3" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_I1_001" + suffix + "\n"
        command += "\t\tmv " + run[name] + "_1" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R1_001" + suffix + "\n"
        command += "\t\tmv " + run[name] + "_2" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R2_001" + suffix + "\n"
        command += "\tfi\n"
        command += "else\n"
        command += "\tmv " + run[name] + "_1" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R1_001" + suffix + "\n"
        command += "\tmv " + run[name] + "_2" + suffix + " " + run[sample] + "_S1_L00" + str(count) + "_R2_001" + suffix + "\n"
        command += "fi\n"
        command += "cd ../../\n"

    # Cellranger count
    command += "\n"
    print("Counting", run[sample] + "...")
    command += "cd " + run[paper] + "\n"
    command += cellranger + " count --id=02_output_" + run[sample] +" --transcriptome=" + transcriptome + " --fastqs=./01_fastq --sample=" + run[sample] + " --no-bam --localcores=16 --localmem=100" + "\n"
    command += "cd ../\n"

    # Delete raw data if delete_after_count is set
    if delete_after_count:
        print("Deleting sra and fastq files...")
        command += "rm " + run[paper] + "/01_fastq/" + run[sample] + "_S1_L00*.fast*" + "\n"
    os.chdir("scripts_outputs_errors")
    file = open(run[sample] + ".sh", "w")
    file.write(command)
    file.close()
    if bash:
        print("bash " + run[sample] + ".sh")
        print(os.popen("bash " + run[sample] + ".sh" + " &>> " + run[sample] + ".sh.output\n").read().rstrip("\n"))
    os.chdir("../")
    print()