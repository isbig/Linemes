# coding=utf-8
from pathlib import Path
import csv
import os
import glob


# path ของโฟลเดอร์นี้
path_here = os.getcwd()

# path ของไฟล์ในโฟลเดอร์ line message chat ที่อยู่ในโฟลเดอร์นี้
all_file = glob.glob(path_here + '/line message chat/*.txt')

directory = path_here + '/line message chat csv'
if not os.path.exists(directory):
    os.makedirs(directory)

# ปรับเปลี่ยนมาจาก
# https://stackoverflow.com/questions/39642082/convert-txt-to-csv-python-script?rq=1
for file in all_file:
    with open(file, 'r') as in_file:
        stripped = (line.strip() for line in in_file)
        lines = (line.split('\t') for line in stripped if line)
        print(str(lines))
        s = os.path.basename(file)
        s1 = os.path.splitext(s)[0]
        with open(os.path.join(directory, s1 + '.csv'), 'w') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(('time', 'name', 'text'))
            writer.writerows(lines)
