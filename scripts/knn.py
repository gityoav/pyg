# -*- coding: utf-8 -*-
import numpy as np
from numpy.random import random, seed
from pyg import as_list

def connected_network(inputs, *sizes):
    network = []
    n = inputs
    for size in as_list(sizes):
        layer = [{'w': random(n), 'b': random()} for _ in range(size)]
        n = size
        network.append(layer)
    return network

def activate(node, inputs):
    return np.sum(node['w'] * inputs) + node['b']

def transfer(activation):
    return 1 / (1 + np.exp(-activation))
    

def forward_propagate(network, inputs):
    for layer in network:
        inputs = np.array(inputs)
        for node in layer:
            node['o'] = transfer(activate(node, inputs))
        inputs = [node['o'] for node in layer]
    return inputs


def transfer_derivative(output):
    return output * (1.0 - output)

def back_propagate(network, expected):
    prev_layer = layer = network[-1]
    errors = [expected[i] - layer[i]['o'] for i in range(len(layer))] ### top level errors
    for j in range(len(layer)):
        node = layer[j]
        node['d'] = errors[j] * transfer_derivative(node['o'])
    for i in reversed(range(len(network)-1)):
        layer = network[i]
        for j in range(len(layer)):
            node = layer[j]
            error = sum([n['w'][j] * n['d'] for n in prev_layer])
            node['d'] = error * transfer_derivative(node['o'])
        prev_layer = layer
    return network


def update_weights(network, inputs, rate = 0.3):
    for layer in network:
        for node in layer:
            for j in range(len(inputs)):
                node['w'][j] += inputs[j] * node['d'] * rate
            node['b'] += rate  * node['d']
        inputs = [node['o'] for node in layer]            
    return network           
    

def total_error(network, expected, p = 1):
    layer = network[-1]
    return sum([abs(expected[i] - layer[i]['o'])**p for i in range(len(layer))])**(1/p) ### top level errors
    

def train_network(network, train_inputs, rate, epochs, train_expected):
    n = len(network[-1])
    expecteds = np.identity(n)
    for epoch in range(epochs):
        for inputs, expect in zip(train_inputs,train_expected):
            _ = forward_propagate(network, inputs)
            expected = expecteds[expect]
            network = back_propagate(network, expected)
            network = update_weights(network, inputs, rate)
        print(total_error(n, expected))
        
    
    
def predict(network, inputs):
    outputs = forward_propagate(network, inputs)
    return outputs.index(max(outputs))

n = connected_network(3,5,2)
inputs = [1,2,-3]
expected = [0,1]
for _ in range(1000):
    i = forward_propagate(n,inputs)
    n = back_propagate(n, expected)
    n = update_weights(n, inputs, rate = 0.5)
    print(total_error(n, expected))
    
[node['o'] for node in n[-1]]



