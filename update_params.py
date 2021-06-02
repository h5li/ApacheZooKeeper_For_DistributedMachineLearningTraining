import numpy as np
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('worker_id', type=str)
args = parser.parse_args()

w = np.zeros((785, 1))
if os.path.exists('params{}.npy'.format(args.worker_id)):
    with open('params{}.npy'.format(args.worker_id), 'rb') as f:
        w = np.load(f)

with open('grads{}.txt'.format(args.worker_id), 'r') as f:
    for i, line in enumerate(f):
        w[i][0] += float(line.strip())

with open('params{}.npy'.format(args.worker_id), 'wb') as f:
    np.save(f, w)