import argparse
import numpy as np
import os
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('data_file', type=str)
parser.add_argument('worker_id', type=str)
args = parser.parse_args()

df = pd.read_csv(args.data_file, header=None)

X = np.array(df[df[0] == 0])[:, 1:]
y = np.zeros((X.shape[0], 1))
X = np.vstack((X, np.array(df[df[0] == 1])[:, 1:]))
X = np.hstack((np.ones((X.shape[0], 1)), X))
y = np.vstack((y, np.ones((len(df[df[0] == 1].index), 1))))

def h(w):
    return 1 / (1 + np.exp(-np.matmul(X, w)))

w = np.zeros((785, 1))
if os.path.exists('params{}.npy'.format(args.worker_id)):
    with open('params{}.npy'.format(args.worker_id), 'rb') as f:
        w = np.load(f)

gradient = -0.00001 * np.sum(np.multiply(X, h(w) - y), axis=0)
with open('grads'+args.worker_id+'.txt', 'w+') as f:
    for i in range(785):
        f.write(str(gradient[i]) + '\n')
