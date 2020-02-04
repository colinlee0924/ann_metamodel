import os
import time
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader, Dataset, Subset
#from torch.utils.tensorboard import SummaryWriter

# fixed the seed for reproducibility
seed = 999
random.seed(seed)
np.random.seed(seed)
torch.manual_seed(seed)
torch.backends.cudnn.deterministic = True

# some constants
CHECK = False # True #
ROOT_DIR = Path(os.getcwd()) / 'assets'

# hyperparameters
MAX_EPOCH = 1000 # 2000 # 682 #

def mean_absolute_precentage_error(y_pred, y_target):
    y_target = y_target.view_as(y_pred)
    e = torch.abs(y_target - y_pred) / torch.abs(y_target)
    return 100.0 * torch.mean(e).item()

class TrainData(Dataset):
    def __init__(self, csv_path):
        super().__init__()
        self.anns = pd.read_csv(csv_path).to_dict('records')

    def __len__(self):
        '''Return the number of sample
        '''
        return len(self.anns)

    def __getitem__(self, idx):
        '''Map index 'idx' to a sample, i.e., a design case and its performances
        Args:
            idx: (int) index
        Return:
            dcase: (torch.FloatTensor) design case
            kpi  : (torch.FloatTensor) performance
        '''
        ann = self.anns[idx]
        dcase = [ann['AV1'],
                 ann['AV2'],
                 ann['AV3'],
                 ann['DD_factor'],
                 ann['DP_rule']
                 ]
        dcase = torch.tensor(dcase).float()
        kpi = [ann['Uti_A'],
               ann['Uti_B'],
               ann['Uti_C'],
               ann['Uti_D']] # [ann['avgWIP']] # [ann['avgTardi']] #
        kpi = torch.tensor(kpi).float()

        return dcase, kpi

class TestData(TrainData):
    def __init__(self, csv_path):
        super().__init__(csv_path)

# Do some check for TrainData
if CHECK is True:
    # train_dir = Path(os.getcwd() + '/assets/training_data')
    # data = TrainData(train_dir / 'train_labels.csv')
    data = TrainData(ROOT_DIR / 'train_labels.csv')
    print('========================= Check TrainData ==========================')
    print(len(data))
    dcase, kpi = data[-1]
    print(dcase.size())
    print(kpi.size())
    print('------'); print('case: '); print(dcase)
    print('------------'); print('performance: '); print(kpi)
    print('====================================================================')
    print()



class Net(nn.Module):
    def __init__(self):
        '''Defines parameters (what layers you gonna use)
        '''
        super().__init__()
        self.fc = nn.Linear(3, 1, bias=True)
        self.regression1 = nn.Sequential(
            nn.Linear(5, 55), nn.ReLU(), nn.Linear(55, 4)#, nn.Sigmoid()
        )
        self.regression2 = nn.Sequential(
            nn.Linear(5, 6), nn.ReLU(), nn.Linear(6, 7), nn.ReLU(),
            nn.Linear(7, 6), nn.ReLU(), nn.Linear(6,1)#, nn.Sigmoid()
        )
        self.regression3 = nn.Sequential(
            nn.Linear(5, 12), nn.ReLU(), nn.Linear(12, 14), nn.ReLU(),
            nn.Linear(14, 8), nn.ReLU(), nn.Linear(8,1)#, nn.Sigmoid()
        )
        self.regression4 = nn.Sequential(
            nn.Linear(5, 6), nn.ReLU(), nn.Linear(6, 7), nn.ReLU(),
            nn.Linear(7, 8), nn.ReLU(), nn.Linear(8, 7), nn.ReLU(),
            nn.Linear(7, 6), nn.ReLU(), nn.Linear(6, 1)#, nn.Sigmoid()
        )
        self.regression5 = nn.Sequential(
            nn.Linear(5, 20), nn.ReLU(), nn.Linear(20, 21), nn.ReLU(),
            nn.Linear(21, 22), nn.ReLU(), nn.Linear(22, 11), nn.ReLU(),
            nn.Linear(11, 6), nn.ReLU(), nn.Linear(6, 1)#, nn.Sigmoid()
        )
        self.regression6 = nn.Sequential(
            nn.Linear(5, 66), nn.ReLU(), nn.Linear(66, 77), nn.ReLU(),
            nn.Linear(77, 66), nn.ReLU(), nn.Linear(66,1)#, nn.Sigmoid()
        )

    def forward(self, dcase_b):
        '''Define how layers are interact, that is, the forward function.
        Args:
            dcase_b: design cases (mini-batch), shaped [N, 3]
        Return:
            pred_b: the predictions (mini-batch) of the performances, shaped [N, 1]
        '''
        pred_b = self.regression1(dcase_b)
        # pred_b = self.regression1(dcase_b)
        # pred_b = self.fc(dcase_b)#.flatten() # [N, 1] -> [N,]
        return pred_b

# Do some check
if CHECK is True:
    print('========================= Check DataLoader ==========================')
    loader = DataLoader(data, batch_size=10)
    dcase_b, target_b = next(iter(loader))
    print(dcase_b.size())
    print(target_b.size())

    # Do a forward
    device = 'cpu'
    model = Net().to(device)
    criterion = nn.L1Loss()

    dcase_b = dcase_b.to(device)
    target_b = target_b.to(device)
    pred_b = model(dcase_b)
    loss = criterion(pred_b, target_b)
    print(loss)
    print('====================================================================')
    print()


class Trainer:
    def __init__(self, log_dir):
        '''Initialize the varibles for training
        Args:
            log_dir: (pathlib.Path) the direction used for logging
        '''
        self.log_dir = log_dir

        # Datasets and dataloaders
        data = TrainData(ROOT_DIR / 'train_labels.csv')
        # 1. Split the whole training data into train and valid (validation)
        pivot = len(data) * 7 // 10
        self.train_set = Subset(data, range(0, pivot))
        self.valid_set = Subset(data, range(pivot, len(data)))
        # 2. Make the corresponding dataloaders
        self.train_loader = DataLoader(self.train_set, 50, shuffle=True, num_workers=2)
        self.valid_loader = DataLoader(self.valid_set, 10, shuffle=False, num_workers=2)

        # model, loss function, optimizer
        self.device = 'cpu'
        self.model = Net().to(self.device)
        self.criterion = nn.L1Loss() # nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=5e-3)
        self.max_epoch = MAX_EPOCH #5 #20

    def run(self):
        metrics = {'train_loss': [], 'valid_loss': [], 'train_acc': [], 'valid_acc': []}

        for self.epoch in tqdm(range(self.max_epoch)):
            self.train_loss, self.train_acc = self.train() # train 1 epoch
            self.valid_loss, self.valid_acc = self.valid() # valid 1 epoch

            # print(f'Epoch {self.epoch:03d}:')
            # print('train loss: {}   acc: {} %'.format(self.train_loss.item(), self.train_acc))
            # print('valid loss: {}   acc: {} %'.format(self.valid_loss.item(), self.valid_acc)); print('')
            metrics['train_loss'].append(self.train_loss.item()); metrics['train_acc'].append(self.train_acc)
            metrics['valid_loss'].append(self.valid_loss.item()); metrics['valid_acc'].append(self.valid_acc)

        # Save the parameters(weights) of the model to disk
        self.weights = self.model.state_dict()
        torch.save(self.weights, self.log_dir / 'model_weights.pth')
        pd.DataFrame(metrics).to_excel(self.log_dir / 'train_log.xlsx')
        # print('Finished Training')

        # Plot the loss curve against epoch
        fig, ax = plt.subplots(1, 1, sharex=True)
        ax.plot(range(self.max_epoch), np.array(metrics['train_loss']), label='train_loss')
        ax.plot(range(self.max_epoch), np.array(metrics['valid_loss']), label='valid_loss')
        ax.set_ylabel("loss")
        ax.set_xlabel("epoch")
        ax.legend(loc='upper right')
        fig.savefig(log_dir / 'lossCurve.png')

    def train(self):
        self.model.train()
        step = 0; losses = 0; mape_total = 0
        for inp_b, tgt_b in (iter(self.train_loader)):
            inp_b = inp_b.to(self.device)
            tgt_b = tgt_b.to(self.device)

            # Standard steps of training flow
            self.optimizer.zero_grad()
            pred_b = self.model(inp_b)
            loss = self.criterion(pred_b, tgt_b)
            loss.backward()
            self.optimizer.step()

            mape = mean_absolute_precentage_error(pred_b, tgt_b)

            # To compute the average loss
            mape_total += mape
            losses += loss
            step += 1
        return (losses / step), 100 - (mape_total / step)

    @torch.no_grad()
    def valid(self):
        self.model.eval()
        step = 0; losses = 0; mape_total = 0
        for inp_b, tgt_b in (iter(self.valid_loader)):
            inp_b = inp_b.to(self.device)
            tgt_b = tgt_b.to(self.device)

            # Just do forwarding
            pred_b = self.model(inp_b)
            loss = self.criterion(pred_b, tgt_b)
            mape = mean_absolute_precentage_error(pred_b, tgt_b)

            # To compute the average loss
            mape_total += mape
            losses += loss
            step += 1
        return (losses / step), 100 - (mape_total / step)

# Do some check
if CHECK is True:
    print("========================= Let's do Training ==========================")
    log_dir = Path('./runs/') / f'{datetime.now():%b.%d %H:%M:%S}'
    log_dir.mkdir(parents=True)
    Trainer(log_dir).run()
    print('======================================================================')
    print()

    test_data = TestData(ROOT_DIR / 'test_labels.csv')
    print('========================= Check TestData ==========================')
    print(len(data))
    dcase, tgt = data[-1]
    print(dcase.size())
    print(tgt.size())
    print('------'); print('case: '); print(dcase)
    print('------------'); print('performance: '); print(tgt)
    print('====================================================================')
    print()


class Tester:
    def __init__(self, csv_path, model, criterion, device='cpu'):
        self.test_data = TestData(Path(csv_path))
        self.data_loader = DataLoader(self.test_data, batch_size=10)
        # hyperparameters
        self.model = model
        self.criterion = criterion
        self.device = device
        # statistics
        self.loss, self.mape = 0, 0

    def run(self):
        with torch.no_grad():
            self.model.eval()
            step, losses, mape_total = 0, 0 ,0
            for inp_b, tgt_b in (iter(self.data_loader)):
                inp_b = inp_b.to(self.device)
                tgt_b = tgt_b.to(self.device)

                # Just do forwarding
                pred_b = self.model(inp_b)
                loss = self.criterion(pred_b, tgt_b)
                loss = loss.cpu()
                loss = loss.detach().numpy()
                # MAPE
                mape = mean_absolute_precentage_error(pred_b, tgt_b)

                # To compute the average loss and maperror
                mape_total += mape
                losses += loss
                step += 1
            self.loss = (losses / step)
            self.mape = (mape_total / step)


if __name__ == '__main__':
    # Do training
    if CHECK is False:
        print("========================= Let's do Training ==========================")
        log_dir = Path('./runs/') / f'{datetime.now():%b.%d %H:%M:%S}'
        log_dir.mkdir(parents=True)

        t0 = time.process_time()

        trainer = Trainer(log_dir)
        trainer.run()

        t_end = time.process_time()
        print('====================')
        print('* Training results *')
        print('====================')
        print('Train Loss: {}   acc: {} %'.format(trainer.train_loss.item(), trainer.train_acc))
        print('Valid Loss: {}   acc: {} %'.format(trainer.valid_loss.item(), trainer.valid_acc))
        print('Cost {} seconds.'.format(t_end - t0)); print()
        print()

    # # prepare the testing model
    # device = 'cpu'
    # metamodel = Net().to(device)
    # metamodel.load_state_dict(torch.load(log_dir / 'model_weights.pth'))
    # criterion = nn.L1Loss()
    #
    # # Do testing
    # print('===========')
    # print('* Testing *')
    # print('===========')
    # tester = Tester(ROOT_DIR / 'test_labels.csv', metamodel, criterion)
    # t0 = time.process_time()
    # tester.run()
    # t_end = time.process_time()
    # print('Testing Loss: ', tester.loss)
    # print("Acc         : {} %".format(100 - tester.mape))
    # print('Cost {} seconds.'.format(t_end - t0)); print()
