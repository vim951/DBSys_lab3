from matplotlib import pyplot as plt

##

N = [100, 500, 1000, 2000, 5000]

NLJ1 = [116, 1198, 2801, 9060, 40311]
NLJ2 = [226, 2004, 2978, 8224, 44168]

IE1 = [61, 133, 443, 2437, 11390]
IE2 = [87, 501, 820, 3923, 17018]

IE1_optim = [38, 211, 574, 1626, 9704]
IE2_optim = [23, 205, 794, 2483, 15918]

##

plt.plot(N, NLJ2, label='NLJ')
plt.plot(N, IE2, label='IE')
plt.plot(N, IE2_optim, label='IE optim')

plt.title('Query 2b')
plt.xlabel('Number of tuples')
plt.ylabel('Time (ms)')

plt.legend()
plt.show()