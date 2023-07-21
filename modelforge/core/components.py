from abc import ABC, abstractmethod

class MFModel(ABC):
    @abstractmethod
    def train(self,datasource):
        pass
    @abstractmethod
    def predict_batch(self,data):
        pass
    @abstractmethod
    def save(self,output_directory):
        pass

class MFDatasource(ABC):
    @abstractmethod
    def get_data(self,training=False):
        pass

    @property
    def label(self):
        raise NotImplementedError("Subclass must implement the 'label' property")
    
    @property
    def features(self):
        raise NotImplementedError("Subclass must implement the 'features' property")

    def set_interval(self,start,end):
        self.interval = (start,end)
