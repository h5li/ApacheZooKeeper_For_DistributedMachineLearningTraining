import argparse
import numpy as np
import os
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('data_file', type=str)
parser.add_argument('iterations', type=int)
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
for i in range(args.iterations):
    w -= 0.00001 * np.sum(np.multiply(X, h(w) - y), axis=0, keepdims=True).T

with open('params.npy', 'wb') as f:
    np.save(f, w)
