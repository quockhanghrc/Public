import copy
import os
import time

import torch

from modeling.trainer import MetricCallback, InferenceCallback
from modeling.utils import create_logger, TensorboardWriter, DEVICE

LOGGER = create_logger(name=__name__)


class Trainer:
    def __init__(
            self,
            experiment_name,
            train_dataloader,
            validation_dataloader,
            eval_dataloader,
            model,
            optimizer,
            loss_function,
            ranking_metrics,
            epoch_cnt=None,
            step_cnt=None,
            best_metric=None,
            epochs_threshold=40,
            valid_step=256,
            eval_step=256,
            log_steps=None,
            checkpoint_dir='checkpoints'
    ):
        self._experiment_name = experiment_name
        self._train_dataloader = train_dataloader
        self._validation_dataloader = validation_dataloader
        self._eval_dataloader = eval_dataloader
        self._model = model
        self._optimizer = optimizer
        self._loss_function = loss_function
        self._epoch_cnt = epoch_cnt
        self._step_cnt = step_cnt
        self._best_metric = best_metric
        self._epochs_threshold = epochs_threshold
        self._ranking_metrics = ranking_metrics
        self._checkpoint_dir = checkpoint_dir
        self._log_steps = log_steps if log_steps else 10**9  # None => no periodic step logs
        os.makedirs(self._checkpoint_dir, exist_ok=True)

        tensorboard_writer = TensorboardWriter(self._experiment_name)

        self._metric_callback = MetricCallback(tensorboard_writer=tensorboard_writer, on_step=1)

        self._validation_callback = InferenceCallback(
            tensorboard_writer=tensorboard_writer,
            step_name='validation',
            model=model,
            dataloader=validation_dataloader,
            on_step=valid_step,
            metrics=ranking_metrics,
            pred_prefix='predictions',
            labels_prefix='labels'
        )

        self._eval_callback = InferenceCallback(
            tensorboard_writer=tensorboard_writer,
            step_name='eval',
            model=model,
            dataloader=eval_dataloader,
            on_step=eval_step,
            metrics=ranking_metrics,
            pred_prefix='predictions',
            labels_prefix='labels'
        )

    def train(self):
        step_num = 0
        epoch_num = 0
        current_metric = 0
        best_epoch = 0
        best_checkpoint = None
        max_steps = self._step_cnt if self._step_cnt is not None else 200_000

        LOGGER.debug('Start training...')

        while (step_num < max_steps):
            if best_epoch + self._epochs_threshold < epoch_num:
                LOGGER.debug(
                    'There is no progress during {} epochs. Finish training'.format(self._epochs_threshold))
                break

            LOGGER.debug(f'Start epoch {epoch_num}')
            print(f"[train] === epoch {epoch_num} start ===", flush=True)
            epoch_loss_sum = 0.0
            epoch_loss_cnt = 0
            epoch_start = time.time()
            sw = time.time()  # sliding window start for rate/ETA
            for batch in self._train_dataloader:
                self._model.train()

                # Move to device
                for key, values in batch.items():
                    batch[key] = values.to(DEVICE)

                # Forward step
                batch.update(self._model(batch))
                loss = self._loss_function(batch)

                # Backward step
                self._optimizer.zero_grad()
                loss.backward()
                self._optimizer.step()

                # Callbacks (skip step 0: a full-dataset eval on an untrained model is wasted
                # compute, especially on CPU. The final eval is run separately via self.eval().)
                validation_metrics = self._validation_callback(step_num) if step_num > 0 else {}
                evaluation_metrics = self._eval_callback(step_num) if step_num > 0 else {}

                epoch_loss_sum += loss.item()
                epoch_loss_cnt += 1

                # Log metrics
                self._metric_callback(key='loss', value=loss.item(), step_num=step_num, prefix='train')
                for key, value in validation_metrics.items():
                    self._metric_callback(key=key, value=value, step_num=step_num, prefix='validation')
                for key, value in evaluation_metrics.items():
                    self._metric_callback(key=key, value=value, step_num=step_num, prefix='eval')

                step_num += 1
                if step_num % self._log_steps == 0:
                    now = time.time()
                    rate = self._log_steps / max(now - sw, 1e-6)
                    eta_min = (max_steps - step_num) / max(rate, 1e-6) / 60
                    print(
                        f"[train] epoch {epoch_num} step {step_num}/{max_steps} "
                        f"loss {loss.item():.4f} | {rate:.1f} step/s | "
                        f"total {now - epoch_start:.0f}s | ETA ~{eta_min:.1f}m",
                        flush=True,
                    )
                    sw = now

                # Update best checkpoint
                if self._best_metric is None:  # If no best metric is provided last checkpoint is taken
                    best_checkpoint = copy.deepcopy(self._model.state_dict())
                    best_epoch = epoch_num
                    assert False
                elif (
                    validation_metrics
                    and self._best_metric in validation_metrics
                    and (best_checkpoint is None  # If no best checkpoint exists this one is taken
                         or current_metric <= validation_metrics[self._best_metric])  # or if metrics improved
                ):
                    print(step_num, validation_metrics[self._best_metric], current_metric, evaluation_metrics[self._best_metric])
                    current_metric = validation_metrics[self._best_metric]
                    best_checkpoint = copy.deepcopy(self._model.state_dict())
                    best_epoch = epoch_num

            # end of epoch — print per-epoch summary
            if epoch_loss_cnt:
                print(
                    f"[train] === epoch {epoch_num} done: avg_loss {epoch_loss_sum / epoch_loss_cnt:.4f} "
                    f"in {time.time() - epoch_start:.0f}s ===",
                    flush=True,
                )
            epoch_num += 1
        LOGGER.debug('Training procedure has been finished!')
        if best_checkpoint is None:
            # No validation eval ran during training (evals deferred to the end, e.g. large
            # valid_step/eval_step). Fall back to the final trained weights so load() works.
            LOGGER.debug('No validation eval during training; using final weights as best checkpoint.')
            best_checkpoint = copy.deepcopy(self._model.state_dict())
        return best_checkpoint

    def eval(self):
        evaluation_metrics = self._eval_callback(0)
        for key, value in evaluation_metrics.items():
            print(key, value)

    def save(self):
        LOGGER.debug('Saving model...')
        checkpoint_path = f'{self._checkpoint_dir}/{self._experiment_name}_final_state.pth'
        torch.save(self._model.state_dict(), checkpoint_path)
        LOGGER.debug('Saved model as {}'.format(checkpoint_path))

    def load(self, checkpoint):
        self._model.load_state_dict(checkpoint)
