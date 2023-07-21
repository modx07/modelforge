import os
import pandas as pd

from modelforge.utils.registry import object_from_registry
from dask.distributed import Client

class MFRunner:
    def __init__(self, config, distributed=False, dask_scheduler=None):
        self.config = config
        # Make sure the json has all the required fields
        required_fields = ['train_mode', 'model', 'model_params', 'output_dir']
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f'config file is missing required field {field}')

        #TODO - figure out how this works
        self.distributed = distributed
        self.dask_scheduler = dask_scheduler
        self.intervals = dict()

        #TODO - figure out how this works
        # If distributed, create a dask client
        if self.distributed:
            self.client = Client(dask_scheduler)
        
        # Raise error if mode is not supported
        if self.config['train_mode'] not in ['single_window', 'rolling_window']:
            raise ValueError(f'Mode {self.config["train_mode"]} is not supported')
        
        if self.config['train_mode'] == 'rolling_window':
            # Make sure the json has all the required fields
            required_rolling_fields = ['start_date', 'end_date', 'train_period', 'gap_period', 'eval_period', 'recalibration_freq']
            for field in required_rolling_fields:
                if field not in self.config:
                    raise ValueError(f'config file is missing required field {field} - required for rolling tests')

            current_start = pd.to_datetime(self.config['start_date'])
            total_end = pd.to_datetime(self.config['end_date'])

            k = 0
            while True:
                train_start = current_start
                train_end = train_start + pd.to_timedelta(days=self.config['train_period'])
                test_start = train_end + pd.to_timedelta(days=self.config['gap_period'])
                test_end = test_start + pd.to_timedelta(days=self.config['eval_period'])

                test_end_cut = min(test_end, total_end)

                train_interval = (train_start, train_end)
                test_interval = (test_start, test_end_cut)

                self.intervals[k] = {'fit': train_interval, 'eval': test_interval}

                k += 1
                current_start = current_start + pd.to_timedelta(days=self.config['recalibration_freq'])

                if test_end >= total_end:
                    break
        else:
            # Make sure the json has all the required fields
            required_single_fields = ['train_start_date', 'train_end_date', 'eval_start_date', 'eval_end_date']
            for field in required_single_fields:
                if field not in self.config:
                    raise ValueError(f'config file is missing required field {field} - required for rolling tests')
                
            train_interval = (pd.to_datetime(self.config['train_start_date']), pd.to_datetime(self.config['train_end_date']))
            test_interval = (pd.to_datetime(self.config['eval_start_date']), pd.to_datetime(self.config['eval_end_date']))
            self.intervals[0] = {'fit': train_interval, 'eval': test_interval}
        
        self.train()
    
    # TODO - needs to run in the runners folder
    def train(self):
        self.datasource_class = object_from_registry(self.config['datasource'],os.path.join(self.config['database_path'],'registry.db'))
        self.model_class = object_from_registry(self.config['datasource'],os.path.join(self.config['database_path'],'registry.db'))

        for k in self.intervals:
            interval = self.intervals[k]
            if self.distributed:
                self.client.submit(self._train_interval, interval,k) #TODO - fix this
            else:
                trained_model = self._train_interval(interval,k)
                self.intervals[k]['model'] = trained_model

    def _train_interval(self,interval,k):
            outpath = os.path.join(self.config['output_dir'],f'eval_{k}')

            train_start, train_end = interval['fit']
            eval_start, eval_end = interval['eval']

            datasource = self.datasource_class(params=self.config.get('datasource_params',{}))
            datasource.set_interval(train_start, train_end)

            model_instance = self.model_class(params=self.config['model_params'])
            model_instance.train(datasource)

            datasource.set_interval(eval_start, eval_end)
            test_pred = model_instance.predict_batch(datasource)
            test_pred.to_hdf(os.path.join(outpath,'pred.h5'), key='data', mode='w')

            model_instance.save(outpath)
            return model_instance
