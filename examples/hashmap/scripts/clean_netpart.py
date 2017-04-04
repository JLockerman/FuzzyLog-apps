t = 0
interval = 100
data = []
with open('data/same_site_1.txt') as f:
        same_site = f.readlines()

with open('data/diff_site_1.txt') as f:
        diff_site = f.readlines()

for i, tput in enumerate(same_site): 
        tput2 = diff_site[i]
        data.append("%d %s %s" % (t, 10*int(tput) / 1000.0, 10 * int(tput2) / 1000.0))
        t += interval

with open('realgraphs/real_netpart.in', 'w') as f:
        for d in data:
                f.write(d + '\n')
