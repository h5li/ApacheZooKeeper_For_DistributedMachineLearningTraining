import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('num_workers', type=int)
args = parser.parse_args()

df = pd.read_csv('data/zerones.csv', header=None)

zeros = df[df[0] == 0]
ones = df[df[0] == 1]

zeros_per = int(zeros.shape[0] / args.num_workers)
ones_per = int(ones.shape[0] / args.num_workers)

for worker in range(args.num_workers):
    with open('data/w' + str(worker) + 'data.csv', 'w') as f:
        for i in range(worker * zeros_per, (worker + 1) * zeros_per):
            f.write(",".join(str(p) for p in zeros.iloc[i]) + '\n')
        for i in range(worker * ones_per, (worker + 1) * ones_per):
            f.write(",".join(str(p) for p in ones.iloc[i]) + '\n')
