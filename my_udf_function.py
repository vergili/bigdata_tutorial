import sys
sys.path.append('/opt/deploy/udfs')


# Read the hive columns
for line in sys.stdin:
    line = line.split('\t')

    column1 = line[0].strip()
    column2 = line[1].strip()
    column3 = line[2].strip()
    column4 = line[3].strip()
    column5 = line[4].strip()
    column6 = line[5].strip()
    column7 = line[6].strip()
    column8 = line[7].strip()
    column9 = line[8].strip()
    column10 = line[9].strip()
    column11 = line[10].strip()
    column12 = line[11].strip()
    column13 = line[12].strip()
    column14 = line[13].strip()
    column15 = line[14].strip()
    column16 = line[15].strip()
    column17 = line[16].strip()
    column18 = line[17].strip()
    column19 = line[18].strip()
    column20 = line[19].strip()
    column21 = line[20].strip()
    column22 = line[21].strip()
    column23 = line[22].strip()
    column24 = line[23].strip()
    column25 = line[24].strip()
    column26 = line[25].strip()
    column27 = line[26].strip()
    column28 = line[27].strip()
    column29 = line[28].strip()
    column30 = line[29].strip()
    source_id = line[30].strip()

    new_column1 = column1
    new_column2 = column2
    new_column3 = column3
    new_column4 = column4
    new_column5 = column5
    new_column6 = column6
    new_column7 = column7
    new_column8 = column8
    new_column9 = column9
    new_column10 = column10
    new_column11 = source_id

    out_columns = [
        new_column1,
        new_column2,
        new_column3,
        new_column4,
        new_column5,
        new_column6,
        new_column7,
        new_column8,
        new_column9,
        new_column10,
        new_column11
    ]
    print '\t'.join(str(v) for v in out_columns)