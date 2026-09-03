"""Shared RQ-KMeans tokenizer (residual vector quantization over N codebooks)."""
import numpy as np
from sklearn.cluster import KMeans


class RQKMeans:
    def __init__(self, num_clusters, num_codebooks, init="k-means++",
                 max_iter=300, tol=1e-4, verbose=0, random_state=42):
        self.models = [
            KMeans(n_clusters=num_clusters, init=init, max_iter=max_iter,
                   tol=tol, verbose=verbose, random_state=random_state + i)
            for i in range(num_codebooks)
        ]

    def fit(self, X, y=None):
        for model in self.models:
            y = model.fit_predict(X)
            X = X - model.cluster_centers_[y]
        return self

    def predict(self, X):
        result = []
        centroids = []
        for model in self.models:
            result.append(model.predict(X))
            centroids.append(model.cluster_centers_[result[-1]])
            X = X - centroids[-1]
        return np.stack(result, axis=-1)