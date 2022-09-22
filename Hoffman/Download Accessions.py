import os
accession_list = open("Accession List.txt", "r")
os.chdir("scripts_outputs_errors")
for line in accession_list:
    line = line.strip()
    file = open(line + ".sh", "w")
    command = "#!/bin/sh\n#$ -cwd\n#$ -o ./\n#$ -V\n#$ -S /bin/bash\n#$ -l h_data=10G,h_rt=24:00:00\n#$ -e ./\n# -pe shared 1\n\n"
    command += "cd ../\n"
    command += "/u/home/w/wanluliu/tools/sratoolkit/sratoolkit.3.0.0-centos_linux64/bin/prefetch " + line + "\n"
    command += "mv /u/home/w/wanluliu/wanluliu/ncbi/Naive_hESC/sra/" + line + ".sra ./\n"
    command += "/u/home/w/wanluliu/tools/sratoolkit/sratoolkit.3.0.0-centos_linux64/bin/fastq-dump --split-files --gzip " + line + ".sra"
    file.write(command)
    file.close()
    print(os.popen("qsub " + line + ".sh").read().rstrip("\n"))
accession_list.close()
