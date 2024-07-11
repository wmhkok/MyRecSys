import torch
from neuralCF import NeuralCFModel

model = NeuralCFModel(1001, 30001, 16)
model.load_state_dict(torch.load('./torchServe/neuralCF/neuralCF.pth', map_location=torch.device('cpu')))
model.eval()

data = {"instances": [{"movieId": 1, "userId": 1}, {"movieId": 2, "userId": 1}]}
instances = data['instances']
movie_ids = [instance['movieId'] for instance in instances]
user_ids = [instance['userId'] for instance in instances]
movie_ids = torch.tensor(movie_ids, dtype=torch.long)
user_ids = torch.tensor(user_ids, dtype=torch.long)

with torch.no_grad():
    outputs = model(movie_ids, user_ids)
print(outputs)