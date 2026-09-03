import copy


class TrainSampler:
    def __init__(self, dataset, prediction_type, max_sequence_length):
        self._dataset = dataset
        self._prediction_type = prediction_type
        self._max_sequence_length = max_sequence_length

        self._transforms = {
            'sasrec': self._all_items_transform,
            'tiger': self._last_item_transform
        }

    def _all_items_transform(self, sample):
        item_sequence = sample['item.ids'][-self._max_sequence_length:][:-1]
        next_item_sequence = sample['item.ids'][-self._max_sequence_length:][1:]
        return {
            'user.ids': sample['user.ids'],
            'user.length': len(sample['user.ids']),
            'item.ids': item_sequence,
            'item.length': len(item_sequence),
            'labels.ids': next_item_sequence,
            'labels.length': len(next_item_sequence)
        }

    def _last_item_transform(self, sample):
        item_sequence = sample['item.ids'][-self._max_sequence_length:][:-1]
        last_item = sample['item.ids'][-self._max_sequence_length:][-1]
        return {
            'user.ids': sample['user.ids'],
            'user.length': len(sample['user.ids']),
            'item.ids': item_sequence,
            'item.length': len(item_sequence),
            'labels.ids': [last_item],
            'labels.length': 1,
        }

    def __getitem__(self, index):
        sample = copy.deepcopy(self._dataset[index])
        return self._transforms[self._prediction_type](sample)

    def __len__(self):
        return len(self._dataset)


class EvalSampler:
    def __init__(self, dataset, max_sequence_length):
        self._dataset = dataset
        self._max_sequence_length = max_sequence_length

    @property
    def dataset(self):
        return self._dataset

    def __len__(self):
        return len(self._dataset)

    def __getitem__(self, index):
        sample = copy.deepcopy(self._dataset[index])

        item_sequence = sample['item.ids'][-self._max_sequence_length:][:-1]
        next_item = sample['item.ids'][-self._max_sequence_length:][-1]

        return {
            'user.ids': sample['user.ids'],
            'user.length': len(sample['user.ids']),
            'item.ids': item_sequence,
            'item.length': len(item_sequence),
            'labels.ids': [next_item],
            'labels.length': 1,
            'visited.ids': sample['item.ids'][:-1],
            'visited.length': len(sample['item.ids'][:-1])
        }
