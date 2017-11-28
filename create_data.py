import csv
import string
import random

COLUMNS = ['column1', 'column2', 'column3', 'column4', 'column5', 'column6', 'column7', 'column8', 'column9',
           'column10',
           'column11', 'column12', 'column13', 'column14', 'column15', 'column16', 'column17', 'column18', 'column19',
           'column20', 'column21', 'column22', 'column23', 'column24', 'column25', 'column26', 'column27', 'column28',
           'column29', 'column30']

with open('mydata.csv', 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(COLUMNS)

    for i in xrange(10000000):
        if i % 10000 == 0:
            print i

        c1 = i
        N = random.randint(10, 30)
        c2 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c3 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c4 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c5 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        c6 = random.randint(555, 10000)
        c7 = random.randint(555, 10000)
        c8 = random.randint(555, 10000)
        c9 = random.randint(555, 10000)

        N = random.randint(10, 30)
        c10 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c11 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c12 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c13 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c14 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c15 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        c16 = random.randint(555, 10000)
        c17 = random.randint(555, 10000)
        c18 = random.randint(555, 10000)
        c19 = random.randint(555, 10000)

        N = random.randint(10, 30)
        c20 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c21 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        c22 = random.randint(555, 10000)

        N = random.randint(10, 30)
        c23 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c24 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        c25 = random.randint(555, 10000)

        N = random.randint(10, 30)
        c26 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c27 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        N = random.randint(10, 30)
        c28 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        c29 = random.randint(555, 10000)

        N = random.randint(10, 30)
        c30 = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

        row = [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22,
               c23, c24, c25, c26, c27, c28, c29, c30]

        writer.writerow(row)



