import torch
from ts.torch_handler.base_handler import BaseHandler
from neuralCF import NeuralCFModel
import logging
import os

logger = logging.getLogger(__name__)

class MyModelHandler(BaseHandler):
    def initialize(self, context):
        self.manifest = context.manifest
        properties = context.system_properties
        model_dir = properties.get("model_dir")
        self.device = torch.device("cuda" if torch.cuda.is_available() and properties.get("gpu_id") is not None else "cpu")
        
        try:
            self.model = NeuralCFModel(1001, 30001, 16)
            model_path = model_dir + '/neuralCF.pth'
            self.model.load_state_dict(torch.load(model_path, map_location=self.device))
            self.model.to(self.device)
            self.model.eval()
            logger.info("Model loaded successfully.")
        except Exception as e:
            logger.error("Error during model loading: %s", e)
            raise e

    def preprocess(self, data):
        try:
            logger.info("Preprocessing data: %s", data)
            instances = data[0]['body']
            movie_ids = [instance['movieId'] for instance in instances]
            user_ids = [instance['userId'] for instance in instances]
            movie_ids_tensor = torch.tensor(movie_ids, dtype=torch.long)
            user_ids_tensor = torch.tensor(user_ids, dtype=torch.long)
            logger.info("Preprocessed movie_ids: %s, user_ids: %s", movie_ids_tensor, user_ids_tensor)
            return movie_ids_tensor, user_ids_tensor
        except Exception as e:
            logger.error("Error during preprocessing: %s", e)
            raise e

    def inference(self, data):
        try:
            logger.info("Inference with data: %s", data)
            movie_ids, user_ids = data
            with torch.no_grad():
                outputs = self.model(movie_ids, user_ids)
            logger.info("Inference outputs: %s", outputs)
            return outputs
        except Exception as e:
            logger.error("Error during inference: %s", e)
            raise e

    def postprocess(self, inference_output):
        try:
            logger.info("Postprocessing output: %s", inference_output)
            predictions = inference_output.squeeze().tolist()
            logger.info("Postprocessed predictions: %s", predictions)
            return [{"data": predictions}]
        except Exception as e:
            logger.error("Error during postprocessing: %s", e)
            raise e
"""
class MyModelHandler(BaseHandler):
    def initialize(self, context):
        self.manifest = context.manifest
        properties = context.system_properties
        model_dir = properties.get("model_dir")
        self.device = torch.device("cuda" if torch.cuda.is_available() and properties.get("gpu_id") is not None else "cpu")
        
        self.model = NeuralCFModel(1001, 30001, 16)
        model_path = os.path.join(model_dir, "neuralCF.pth")
        self.model.load_state_dict(torch.load(model_path, map_location=self.device))
        self.model.to(self.device)
        self.model.eval()

    def preprocess(self, data):
        instances = data['instances']
        movie_ids = [instance['movieId'] for instance in instances]
        user_ids = [instance['userId'] for instance in instances]
        return torch.tensor(movie_ids, dtype=torch.long).to(self.device), torch.tensor(user_ids, dtype=torch.long).to(self.device)

    def inference(self, data):
        movie_ids, user_ids = data
        with torch.no_grad():
            outputs = self.model(movie_ids, user_ids)
        return outputs

    def postprocess(self, inference_output):
        predictions = inference_output.squeeze().tolist()
        return {"predictions": predictions}
"""
_service = MyModelHandler()
