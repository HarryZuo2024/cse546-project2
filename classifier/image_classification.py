import torch
import torchvision
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
from urllib.request import urlopen
from PIL import Image
import numpy as np
import json
import sys
import time
from torchvision.models import ResNet18_Weights

url = str(sys.argv[1])
#img = Image.open(urlopen(url))
img = Image.open(url)

# 旧的加载方式（会产生警告）
# model = models.resnet18(pretrained=True)
# 新的加载方式（不会产生警告）
model = torchvision.models.resnet18(weights=ResNet18_Weights.DEFAULT)

model.eval()
img_tensor = transforms.ToTensor()(img).unsqueeze_(0)
outputs = model(img_tensor)
_, predicted = torch.max(outputs.data, 1)

with open('./imagenet-labels.json') as f:
    labels = json.load(f)
result = labels[np.array(predicted)[0]]
img_name = url.split("/")[-1]
#save_name = f"({img_name}, {result})"
save_name = f"{img_name},{result}"
print(f"{save_name}")
