from abc import abstractmethod
from typing import Tuple, Union, Collection, Any

import numpy as np
import scipy as sc

def cdf(arr: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    arr_sorted = np.sort(arr)
    return arr_sorted, np.arange(len(arr_sorted)) / len(arr_sorted)

class StatisticalTest():
    v_neutral: float
    label_y: str
    label_file: str
    paired: bool = False
    vectorized: Union[bool, None]

    def __init__(self, **kwargs) -> None:
        pass

    @abstractmethod
    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        raise NotImplementedError

    def f_transform_res(self, res: Any, **kwargs) -> Tuple[float, float]:
        return (res[0], res[1])

    def f_transform_gs(self, x: Collection[int], y: Collection[int]) -> Tuple[Collection[int], Collection[int]]:
        return (x, y)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        raise NotImplementedError

class KolmogorovSmirnovPermutTest(StatisticalTest):
    v_neutral=0.
    label_y="$KS$"
    label_file="permut-kolmogorov-smirnov"
    vectorized=False
    n_resamples: int

    def __init__(self, n_resamples: int = 5000, **kwargs) -> None:
        self.n_resamples = n_resamples
        super().__init__(**kwargs)

    @staticmethod
    def _compute_paired_cdfs(
            t_arr: Tuple[Collection[int], Collection[int]])\
                -> Tuple[np.ndarray, np.ndarray]:
        """Copmute CDFs of a pair of arrays over the common range.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            CDFs for each of the two arrays
        """
        x,y = t_arr
        val_min, val_max = np.min([np.min(x), np.min(y)]), np.max([np.max(x), np.max(y)])
        n_vals = val_max - val_min
        return (sc.stats.cumfreq(
            a=arr,
            numbins=n_vals,
            defaultreallimits=(val_min, val_max)).cumcount / len(arr)\
                for arr in t_arr)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        x_cdf, y_cdf = KolmogorovSmirnovPermutTest._compute_paired_cdfs((x,y))
        d_cdf = (y_cdf - x_cdf)
        idx_max_d = np.argmax(np.abs(d_cdf))
        return d_cdf[idx_max_d]

    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        return sc.stats.permutation_test(
            data=(x,y),
            statistic=lambda x,y: KolmogorovSmirnovPermutTest.compute_test_statistic(x=x,y=y),
            n_resamples=self.n_resamples,
            vectorized=None)

    def f_transform_res(self, res: Any, **kwargs) -> Tuple[float, float]:
        return (res.statistic, res.pvalue)

class ContKolmogorovSmirnovPermutTest(StatisticalTest):
    v_neutral=0.
    label_y=r"$KS_{cont}$"
    label_file="permut-cont-kolmogorov-smirnov"
    vectorized=False
    n_resamples: int

    def __init__(self, n_resamples: int = 5000, **kwargs) -> None:
        self.n_resamples = n_resamples
        super().__init__(**kwargs)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        res = sc.stats.kstest(x,y,**kwargs)
        return (-1) * res.statistic_sign * res.statistic

    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        return sc.stats.permutation_test(
            data=(x,y),
            statistic=lambda x,y: ContKolmogorovSmirnovPermutTest.compute_test_statistic(x=x,y=y),
            n_resamples=self.n_resamples,
            vectorized=None)

    def f_transform_res(self, res: Any, **kwargs) -> Tuple[float, float]:
        return (res.statistic, res.pvalue)

class MannWhitneyPermutTest(StatisticalTest):
    v_neutral=0.5
    label_y = r"$P(B_{m+1} > B_{m})$"
    label_file="permut-mann-whitney"
    vectorized=False
    n_resamples: int

    def __init__(self, n_resamples: int = 5000, **kwargs) -> None:
        self.n_resamples = n_resamples
        super().__init__(**kwargs)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        t = sc.stats.mannwhitneyu(x,y,**kwargs).statistic
        return t / (len(x) * len(y))

    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        return sc.stats.permutation_test(
            data=(x,y),
            statistic=MannWhitneyPermutTest.compute_test_statistic,
            n_resamples=self.n_resamples,
            vectorized=None)

    def f_transform_res(self, res: Any, x: Collection[int], y: Collection[int]) -> Tuple[float, float]:
        return (res.statistic, res.pvalue)

class SpearmanPermutTest(StatisticalTest):
    v_neutral=0.
    label_y = r"Spearman $r$"
    label_file="permut-spearman"
    vectorized=False
    paired=True
    n_resamples: int

    def __init__(self, n_resamples: int = 5000, **kwargs) -> None:
        self.n_resamples = n_resamples
        super().__init__(**kwargs)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        return sc.stats.spearmanr(x,y,**kwargs).statistic

    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        return sc.stats.permutation_test(
            data=(x,y),
            statistic=SpearmanPermutTest.compute_test_statistic,
            n_resamples=self.n_resamples,
            permutation_type="pairings",
            vectorized=None)

    def f_transform_res(self, res: Any, x: Collection[int], y: Collection[int]) -> Tuple[float, float]:
        return (res.statistic, res.pvalue)

class PearsonPermutTest(StatisticalTest):
    v_neutral=0.
    label_y = r"Pearson $r$"
    label_file="permut-pearson"
    vectorized=False
    paired=True
    n_resamples: int

    def __init__(self, n_resamples: int = 5000, **kwargs) -> None:
        self.n_resamples = n_resamples
        super().__init__(**kwargs)

    @staticmethod
    def compute_test_statistic(
            x: Collection[int],
            y: Collection[int], **kwargs) -> float:
        return sc.stats.pearsonr(x,y,**kwargs).statistic

    def f_test(self, x: Collection[int], y: Collection[int], axis: int = 0) -> Any:
        return sc.stats.permutation_test(
            data=(x,y),
            statistic=PearsonPermutTest.compute_test_statistic,
            n_resamples=self.n_resamples,
            permutation_type="pairings",
            vectorized=None)

    def f_transform_res(self, res: Any, x: Collection[int], y: Collection[int]) -> Tuple[float, float]:
        return (res.statistic, res.pvalue)
