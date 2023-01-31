from torch import nn
import torch

import torch.nn.functional as F

from sklearn.preprocessing import StandardScaler
from torchmetrics import Accuracy, F1Score, Precision, Recall
from typing import Any, Dict, List, Type, Union, Optional, Tuple
from torch.utils.data import DataLoader, Dataset
import pandas as pd
import pytorch_lightning as pl

class SigmoidFocalLoss(nn.Module):
    def __init__(self, alpha: float = 0.5, gamma: float = 1.0, reduction: str = "mean"):
        super(SigmoidFocalLoss, self).__init__()
        self.alpha = alpha
        self.gamma = gamma
        self.reduction = reduction
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor):
        """
        Original implementation from https://github.com/facebookresearch/fvcore/blob/master/fvcore/nn/focal_loss.py .
        Loss used in RetinaNet for dense detection: https://arxiv.org/abs/1708.02002.
        Args:
            inputs: A float tensor of arbitrary shape.
                    The predictions for each example.
            targets: A float tensor with the same shape as inputs. Stores the binary
                    classification label for each element in inputs
                    (0 for the negative class and 1 for the positive class).
            alpha: (optional) Weighting factor in range (0,1) to balance
                    positive vs negative examples or -1 for ignore. Default = 0.25
            gamma: Exponent of the modulating factor (1 - p_t) to
                balance easy vs hard examples.
            reduction: 'none' | 'mean' | 'sum'
                    'none': No reduction will be applied to the output.
                    'mean': The output will be averaged.
                    'sum': The output will be summed.
        Returns:
            Loss tensor with the reduction option applied.
        """
        #p = torch.sigmoid(inputs)
        p = inputs
        ce_loss = F.binary_cross_entropy_with_logits(inputs, targets, reduction="none")
        p_t = p * targets + (1 - p) * (1 - targets)
        loss = ce_loss * ((1 - p_t) ** self.gamma)
        if self.alpha >= 0:
            alpha_t = self.alpha * targets + (1 - self.alpha) * (1 - targets)
            loss = alpha_t * loss
        if self.reduction == "mean":
            loss = loss.mean()
        elif self.reduction == "sum":
            loss = loss.sum()
        return loss

class MLP(nn.Module):
    def __init__(self, num_features):
        super(MLP, self).__init__()        # Number of input features is 12.
        self.layer_1 = nn.Linear(num_features, 512) 
        self.layer_2 = nn.Linear(512, 256)
        self.layer_out = nn.Linear(256, 1) 
        
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(p=0.1)
        self.batchnorm1 = nn.BatchNorm1d(512)
        self.batchnorm2 = nn.BatchNorm1d(256)
        
    def forward(self, inputs):
        x = self.relu(self.layer_1(inputs))
        x = self.batchnorm1(x)
        x = self.dropout(x)
        x = self.relu(self.layer_2(x))
        x = self.batchnorm2(x)
        x = self.dropout(x)
        x = self.layer_out(x)
        
        return torch.sigmoid(x).squeeze()

class Model(pl.LightningModule):
    def __init__(self, num_featues):
        super().__init__()
        self.num_featues = num_featues
        self.mlp = MLP(num_featues)
        self.loss_fct = SigmoidFocalLoss()#nn.BCEWithLogitsLoss()
        self.treshold = 0.5
        self.lr = 1e-3
        self.step_size = 10
        self.weight_decay = 0.01
        self.predictions = None
        self.configure_metrics()

    def forward(self, x):
        return self.mlp(x)

    def common_step(self, prefix: str, batch: Any) -> torch.Tensor:
        x, y = batch
        logits = self(x)
        loss = self.loss_fct(logits, y)
        preds = (logits > self.treshold).long()
        if y is not None:
            self.log(f"{prefix}_loss", loss, prog_bar=True, sync_dist=True)
            metric_dict = self.compute_metrics(preds, y, mode=prefix)
            self.log_dict(metric_dict, prog_bar=True, on_step=False, on_epoch=True)
        return {"loss": loss, "pred": preds, "true": y, "logits": logits}

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop.
        x, y = batch
        logits = self(x)
        loss = self.loss_fct(logits, y)
        return loss

    def test_epoch_end(self, outputs) -> None:
        predictions = []
        for d in outputs:
            predictions.extend(d['logits'].cpu().numpy().tolist())
        self.predictions = predictions

    def predict_step(self, batch: Any, batch_idx: int, dataloader_idx: int = 0):
        x, _ = batch
        logits = self(x)
        return logits

    def validation_step(self, batch: Any, batch_idx: int, dataloader_idx: int = 0) -> torch.Tensor:
        return self.common_step("val", batch)

    def test_step(self, batch: Any, batch_idx: int, dataloader_idx: int = 0) -> torch.Tensor:
        return self.common_step("test", batch)

    def compute_metrics(self, preds, labels, mode="val") -> Dict[str, torch.Tensor]:
        # Not required by all models. Only required for classification
        return {f"{mode}_{k}": metric(preds, labels) for k, metric in self.metrics.items()}

    def configure_metrics(self) -> None:
        self.macro_prec = Precision(threshold=self.treshold, task='binary')
        self.macro_recall = Recall(threshold=self.treshold, task='binary')
        self.macro_f1 = F1Score(threshold=self.treshold, task='binary')
        self.acc = Accuracy(threshold=self.treshold, task='binary')
        self.metrics = {"precision": self.macro_prec, "recall": self.macro_recall, "f1": self.macro_f1, "accuracy": self.acc}

    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(self.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        scheduler = torch.optim.lr_scheduler.StepLR(optimizer, self.step_size, gamma=0.3, last_epoch=-1, verbose=False)
        return {
            "optimizer": optimizer,
            "lr_scheduler": {"scheduler": scheduler, "interval": "epoch", "frequency": 1},
        }

class PandasDataset(Dataset):

    def __init__(self, df_or_filename, scaler=None):
        if isinstance(df_or_filename, str):
            df = pd.read_csv(df_or_filename)
        else:
            df = df_or_filename

        self.input_size = len(df.columns) - 1

        input_features = df.columns.to_list()
        input_features.remove('Result')

        x = df.loc[:, input_features].values
        y = df.loc[:, 'Result'].values

        if scaler is None:
            self.scaler = StandardScaler()
            self.scaler.fit(x)
        else:
            self.scaler = scaler

        x = self.scaler.transform(x)
    
        self.x = torch.tensor(x).float()
        self.y = torch.tensor(y).long().float()
    
    def __len__(self):
        return len(self.y)
    
    def __getitem__(self, idx):
        return self.x[idx], self.y[idx]
