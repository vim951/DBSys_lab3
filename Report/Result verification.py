fR = open("/home/victor/Downloads/queriesdata/q.txt", "r")
fS = open("/home/victor/Downloads/queriesdata/q.txt", "r")

R = ''.join(fR.readlines())
S = ''.join(fS.readlines())

R = [[int(y) for y in x.split(',') if y!=''] for x in R.split('\n')]
S = [[int(y) for y in x.split(',') if y!=''] for x in S.split('\n')]

for r in R:
    for s in S:
        if r[2] < s[2] and r[3] < s[3]:
            print('[' + str(r[0]) + ', ' + str(s[0]) + ']')