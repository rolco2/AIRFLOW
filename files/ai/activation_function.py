# -*- coding: utf-8 -*-
"""Untitled3.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1qwrqIE2MzdlaPoIn_BEXjed0JhHB-_zf
"""

# 3.3 활성화 함수
# 시그모이드 함수

import torch
from torch import nn
import matplotlib.pylab as plt

m = nn.Sigmoid() # 시그모이드 함수

x = torch.linspace(-5, 5, 50)
y = m(x)

plt.plot(x,y)
plt.show()

#tanh 하이퍼볼릭 탄젠트

import torch
from torch import nn
import matplotlib.pylab as plt

m = nn.Tanh() # tanh 함수

x = torch.linspace(-5, 5, 50)
y = m(x)

plt.plot(x,y)
plt.show()

# Relu 함수

import torch
from torch import nn
import matplotlib.pylab  as plt

m = nn.ReLU()

x = torch.linspace(-5,5,50)
y = m(x)
plt.plot(x,y)

plt.show()

# 항등 함수

import torch
from torch import nn
import matplotlib.pylab  as plt

x = torch.linspace(-5,5,50)
y = x

plt.plot(x,y)

plt.show()

# 소프트맥스 함수
import torch
from torch import nn
import matplotlib.pylab as plt

m = nn.Softmax(dim=1) # 각 행에서 소프트맥스 함수

x = torch.tensor([[1.0,2.0,3.0], [3.0, 2.0, 1.0]])
y = m(x)

print(y)

# 활성화 함수의 구현

import torch
import matplotlib.pylab as plt

x = torch.linspace(-5, 5, 50)
y = torch.sigmoid(x)

plt.plot(x,y)
plt.show()