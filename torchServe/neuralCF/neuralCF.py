import torch
import torch.nn as nn

class NeuralCFModel(nn.Module):
    def __init__(self, num_movies, num_users, embed_dim):
        super().__init__()
        self.movie_embedding = nn.Embedding(num_movies, embed_dim)
        self.user_embedding = nn.Embedding(num_users, embed_dim)
        self.fc_layers = nn.Sequential()
        for i in range(2):
            self.fc_layers.add_module(f'linear_{i}', nn.Linear(2 * embed_dim, 32))
            self.fc_layers.add_module(f'relu_{i}', nn.ReLU())
        self.output_layer = nn.Linear(32, 1)
    
    def forward(self, movie_id, user_id):
        movie_embed = self.movie_embedding(movie_id)
        user_embed = self.user_embedding(user_id)
        x = torch.cat([movie_embed, user_embed], dim=1)
        x = self.fc_layers(x)
        x = torch.sigmoid(self.output_layer(x))
        return x