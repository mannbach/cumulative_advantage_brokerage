from typing import Optional, Union, Generator, Tuple
from collections import defaultdict

import pandas as pd
import numpy as np
import scipy as sc
from sqlalchemy import select, and_, func, literal
from sqlalchemy.orm import Query

from .grouper import Grouper, GrouperDummy
from .statistical_tests import StatisticalTest
from ..constants import N_RESAMPLES_DEFAULT
from ..dbm import\
    MetricCollaboratorSeriesBrokerageFrequencyComparison,\
    MetricCollaboratorSeriesBrokerageRateComparison,\
    CollaboratorSeriesBrokerage, BinsRealization,\
    Collaboration, Project, Gender, Collaborator,\
    HasSession, CumAdvBrokSession

class CollaboratorSeriesBrokerageComparison(HasSession):
    _df_cs_cached: Union[pd.DataFrame, None]
    id_metric_config_comparison: int
    id_metric_config_career: int
    id_metric_config_impact_group: int
    n_resamples: int
    statistical_test: StatisticalTest
    grouper: Grouper

    def __init__(self, *arg,
                 session: CumAdvBrokSession,
                 id_metric_config_comparison: int,
                 id_metric_config_career: int,
                 id_metric_config_impact_group: int,
                 statistical_test: StatisticalTest,
                 n_resamples: int = N_RESAMPLES_DEFAULT,
                 grouper: Union[None, Grouper] = None,
                 **kwargs) -> None:
        super().__init__(*arg, session=session, **kwargs)
        self.id_metric_config_comparison = id_metric_config_comparison
        self.id_metric_config_career = id_metric_config_career
        self.id_metric_config_impact_group = id_metric_config_impact_group
        self.statistical_test = statistical_test
        self.n_resamples = n_resamples
        self.grouper = grouper if grouper is not None else GrouperDummy()
        self._df_cs_cached = None

    def init_cached_data(self):
        self._log("Loading all data from DB...")
        q_base = self._get_query_base()
        q_values = select(
                q_base.c.id_collaborator,
                q_base.c.stage,
                q_base.c.stage_max_career,
                q_base.c.stage_max_impact,
                q_base.c.decade_birth,
                q_base.c.motif_type,
                q_base.c.role,
                q_base.c.g_dummy,
                q_base.c.gender,
                func.sum(q_base.c.value))\
            .select_from(q_base)\
            .group_by(
                q_base.c.id_collaborator,
                q_base.c.stage,
                q_base.c.stage_max_career,
                q_base.c.stage_max_impact,
                q_base.c.decade_birth,
                q_base.c.motif_type,
                q_base.c.role,
                q_base.c.g_dummy,
                q_base.c.gender)

        _df = defaultdict(list)
        for id_c, stage, stage_max_career, stage_max_impact,\
            decade_birth, motif_type, role, g_dummy, gender, val\
                in self.session.execute(q_values):
            _df["id_collaborator"].append(id_c)
            _df["stage"].append(stage)
            _df["stage_max_career"].append(stage_max_career)
            _df["stage_max_impact"].append(stage_max_impact)
            _df["decade_birth"].append(decade_birth)
            _df["motif_type"].append(motif_type)
            _df["role"].append(role)
            _df["g_dummy"].append(g_dummy)
            _df["gender"].append(gender)
            _df["value"].append(val)

        self._df_cs_cached = pd.DataFrame(_df)
        self._log(f"Cached {len(self._df_cs_cached)} entries.")


    def _get_query_stage_max(self, metric_config: int)\
            -> Query:
        return select(
                CollaboratorSeriesBrokerage.id_collaborator,
                func.max(BinsRealization.position).label("stage_max"))\
            .select_from(CollaboratorSeriesBrokerage)\
            .join(
                BinsRealization, BinsRealization.id == CollaboratorSeriesBrokerage.id_bin)\
            .group_by(CollaboratorSeriesBrokerage.id_collaborator)\
            .where(CollaboratorSeriesBrokerage.id_metric_configuration == metric_config)\
            .subquery()

    def _get_query_base(self) -> select:
        sq_stage_max_career = self._get_query_stage_max(
            self.id_metric_config_career)
        sq_stage_max_impact = self._get_query_stage_max(
            self.id_metric_config_impact_group)

        sq_project_decade = select(
                Collaboration.id_collaborator,
                func.extract("decade", func.min(Project.timestamp)).label("decade_birth"))\
            .select_from(Collaboration)\
            .join(Project, Project.id == Collaboration.id_project)\
            .group_by(Collaboration.id_collaborator)\
            .subquery()

        return select(
            CollaboratorSeriesBrokerage.id,
            CollaboratorSeriesBrokerage.id_collaborator,
            Gender.gender,
            BinsRealization.position.label("stage"),
            sq_stage_max_career.c.stage_max.label("stage_max_career"),
            sq_stage_max_impact.c.stage_max.label("stage_max_impact"),
            sq_project_decade.c.decade_birth.label("decade_birth"),
            CollaboratorSeriesBrokerage.motif_type,
            CollaboratorSeriesBrokerage.role,
            literal(GrouperDummy.possible_values[0]).label("g_dummy"),
            CollaboratorSeriesBrokerage.value)\
        .select_from(CollaboratorSeriesBrokerage)\
        .join(
            BinsRealization, BinsRealization.id == CollaboratorSeriesBrokerage.id_bin)\
        .join(
            Collaborator, Collaborator.id == CollaboratorSeriesBrokerage.id_collaborator)\
        .join(
            Gender, Gender.id == Collaborator.id_gender)\
        .join(
            sq_stage_max_career, sq_stage_max_career.c.id_collaborator == CollaboratorSeriesBrokerage.id_collaborator
        )\
        .join(
            sq_stage_max_impact, sq_stage_max_impact.c.id_collaborator == CollaboratorSeriesBrokerage.id_collaborator
        )\
        .join(
            sq_project_decade, sq_project_decade.c.id_collaborator == CollaboratorSeriesBrokerage.id_collaborator
        )\
        .where(CollaboratorSeriesBrokerage.id_metric_configuration == self.id_metric_config_career)

    def get_values(
            self, stage_curr: int, stage_max: int,
            verbose: bool = True, grouping_key: Union[None, str] = None,
            **kwargs) -> np.ndarray:
        if self._df_cs_cached is not None:
            df_cached_filtered = self._df_cs_cached.loc[
                    (self._df_cs_cached["stage"] == stage_curr)\
                    & (self._df_cs_cached["stage_max_impact"] == stage_max)\
                    & (self._df_cs_cached["stage_max_career"] > stage_curr)\
                    & self.grouper.add_constraints_cached(df=self._df_cs_cached, grouping_key=grouping_key),
                        ["id_collaborator", "value"]]\
                .groupby("id_collaborator")["value"].sum()
            return df_cached_filtered.index.values, df_cached_filtered.values

        q_base = self._get_query_base()
        q_values = select(
                q_base.c.id_collaborator,
                func.sum(q_base.c.value))\
            .select_from(q_base)\
            .where(
                and_(
                    q_base.c.stage == stage_curr,
                    q_base.c.stage_max_impact == stage_max,
                    q_base.c.stage_max_career > stage_curr,
                    *self.grouper.add_constraints(q_base=q_base, grouping_key=grouping_key)))\
            .group_by(q_base.c.id_collaborator)

        idc = []
        vals = []
        for id_collaborator, val in self.session.execute(q_values):
            vals.append(val)
            idc.append(id_collaborator)
        return np.asarray([idc, vals], dtype=int)

    def _log(self, msg: str, verbose: bool = True):
        if verbose:
            print("\t\t", msg)

    def compute_comparison(
            self, stage_curr: int, stage_max_curr: int, stage_max_next: int,
            verbose: bool = True, grouping_key: Union[None, str] = None, **kwargs) -> MetricCollaboratorSeriesBrokerageFrequencyComparison:
        _, a_vals_stage_max_curr = self.get_values(stage_curr=stage_curr, stage_max=stage_max_curr, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(m)={len(a_vals_stage_max_curr)}")
        _, a_vals_stage_max_next = self.get_values(stage_curr=stage_curr, stage_max=stage_max_next, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(m+1)={len(a_vals_stage_max_next)}")

        if len(a_vals_stage_max_curr) == 0 or len(a_vals_stage_max_next) == 0:
            return None

        _, t, p, ci = self._perform_test(a_vals_stage_max_next, a_vals_stage_max_curr)

        return MetricCollaboratorSeriesBrokerageFrequencyComparison(
            id_metric_configuration=self.id_metric_config_comparison,
            stage=stage_curr,
            max_stage_curr=stage_max_curr,
            max_stage_next=stage_max_next,
            grouping_key=grouping_key,
            test_statistic=t,
            p_value=p,
            # ci_low=-np.inf,
            # ci_high=np.inf,
            ci_low=ci.low if ci is not None else None,
            ci_high=ci.high if ci is not None else None,
            n_x=len(a_vals_stage_max_next),
            mu_x=np.mean(a_vals_stage_max_next),
            std_x=np.std(a_vals_stage_max_next),
            n_y=len(a_vals_stage_max_curr),
            mu_y=np.mean(a_vals_stage_max_curr),
            std_y=np.std(a_vals_stage_max_curr),
        )

    def generate_comparisons(self, n_stages: int, verbose: bool = True, **kwargs)\
        -> Generator[MetricCollaboratorSeriesBrokerageFrequencyComparison, None, None]:
        self._log(
            msg=f"Starting comparison with grouper {self.grouper.name}",
            verbose=verbose)
        for stage in range(n_stages - 1):
            self._log(msg=f"Stage s={stage}", verbose=verbose)
            for stage_max_curr in range(n_stages - 1):
                stage_max_next = stage_max_curr + 1
                self._log(
                    msg=f"\tMax stages m={stage_max_curr}, m+1={stage_max_next}",
                    verbose=verbose)
                result = self.compute_comparison(stage, stage_max_curr, stage_max_next, verbose=verbose, **kwargs)

                if result is None:
                    continue

                self._log(msg="Result:", verbose=verbose)
                self._log(msg=f"\tstage=`{result.stage}`", verbose=verbose)
                self._log(msg=f"\tmax_stage_curr=`{result.max_stage_curr}`", verbose=verbose)
                self._log(msg=f"\tmax_stage_next=`{result.max_stage_next}`", verbose=verbose)
                self._log(msg=f"\tgrouping_key=`{result.grouping_key}`", verbose=verbose)
                self._log(msg=f"\ttest_statistic=`{result.test_statistic:.2f}`", verbose=verbose)
                self._log(msg=f"\tp_value=`{result.p_value:.2f}`", verbose=verbose)
                self._log(msg=f"\tci_low=`{result.ci_low:.2f}`", verbose=verbose)
                self._log(msg=f"\tci_high=`{result.ci_high:.2f}", verbose=verbose)
                self._log(msg=f"\tn_x=`{result.n_x}`", verbose=verbose)
                self._log(msg=f"\tmu_x=`{result.mu_x:.2f}`", verbose=verbose)
                self._log(msg=f"\tstd_x=`{result.std_x:.2f}`", verbose=verbose)
                self._log(msg=f"\tn_y=`{result.n_y}`", verbose=verbose)
                self._log(msg=f"\tmu_y=`{result.mu_y:.2f}`", verbose=verbose)
                self._log(msg=f"\tstd_y=`{result.std_y:.2f}`", verbose=verbose)
                yield result

    def _perform_test(self, x: np.ndarray, y: np.ndarray)\
            -> Optional[Tuple[float, float, float, float]]:
        res, t, p, ci = None, None, None, None
        try:
            res = self.statistical_test.f_test(x, y)
            t, p = self.statistical_test.f_transform_res(
                res, x=x, y=y)

            ci = sc.stats.bootstrap(
                data=(x, y),
                statistic=lambda x,y,**kwargs:\
                    self.statistical_test.compute_test_statistic(
                        x=x, y=y, **kwargs),
                n_resamples=self.n_resamples,
                paired=self.statistical_test.paired,
                vectorized=self.statistical_test.vectorized).confidence_interval
        except Exception as err:
            self._log("ERROR occurred when computing tests")

            self._log(f"test={self.statistical_test.label_file}")
            self._log(f"grouper={self.grouper.name}")

            self._log(f"res={res}")
            self._log(f"t={t}")
            self._log(f"p={p}")
            self._log(f"ci_low={ci.low if ci is not None else None}")
            self._log(f"ci_hig={ci.hig if ci is not None else None}")

            self._log(f"n_x={len(x)}")
            self._log(f"mu_x={np.mean(x)}")
            self._log(f"std_x={np.std(x)}")
            self._log(f"n_y={len(y)}")
            self._log(f"mu_y={np.mean(y)}")
            self._log(f"std_y={np.std(y)}")
            self._log(f"Error log: {err}")
        return res, t, p, ci

class CollaboratorSeriesRateStageComparison(CollaboratorSeriesBrokerageComparison):
    a_dt: np.ndarray

    def __init__(
            self, *arg,
            session: CumAdvBrokSession,
            id_metric_config_comparison: int,
            id_metric_config_career: int,
            id_metric_config_impact_group: int,
            bins: np.ndarray,
            statistical_test: StatisticalTest,
            n_resamples: int = N_RESAMPLES_DEFAULT,
            grouper: Optional[Grouper] = None, **kwargs) -> None:
        super().__init__(*arg, session=session,
            id_metric_config_comparison=id_metric_config_comparison,
            id_metric_config_career=id_metric_config_career,
            id_metric_config_impact_group=id_metric_config_impact_group,
            statistical_test=statistical_test, n_resamples=n_resamples,
            grouper=grouper, **kwargs)
        self.a_dt = np.diff(bins)


    def get_values(self, stage_curr: int, stage_max: int, verbose: bool = True, grouping_key: Union[str, None] = None, **kwargs) -> np.ndarray:
        idc, freq = super().get_values(stage_curr, stage_max, verbose, grouping_key, **kwargs)
        return idc, freq / self.a_dt[stage_curr]

    def compute_comparison(
            self,
            stage_curr: int, stage_next: int, stage_max: int,
            verbose: bool = True, grouping_key: Union[str, None] = None, **kwargs) -> MetricCollaboratorSeriesBrokerageRateComparison:

        _, a_vals_stage_curr = self.get_values(stage_curr=stage_curr, stage_max=stage_max, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(s)={len(a_vals_stage_curr)}")
        _, a_vals_stage_next = self.get_values(stage_curr=stage_next, stage_max=stage_max, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(s+1)={len(a_vals_stage_next)}")

        if len(a_vals_stage_curr) == 0 or len(a_vals_stage_next) == 0:
            return None

        _, t, p, ci = self._perform_test(a_vals_stage_next, a_vals_stage_curr)

        return MetricCollaboratorSeriesBrokerageRateComparison(
            id_metric_configuration=self.id_metric_config_comparison,
            stage_next=stage_next,
            stage_curr=stage_curr,
            stage_max=stage_max,
            grouping_key=grouping_key,
            test_statistic=t,
            p_value=p,
            ci_low=ci.low if ci is not None else None,
            ci_high=ci.high if ci is not None else None,
            n_x=len(a_vals_stage_next),
            mu_x=np.mean(a_vals_stage_next),
            std_x=np.std(a_vals_stage_next),
            n_y=len(a_vals_stage_curr),
            mu_y=np.mean(a_vals_stage_curr),
            std_y=np.std(a_vals_stage_curr),
        )

    def generate_comparisons(self, n_stages: int, verbose: bool = True, **kwargs) \
            -> Generator[MetricCollaboratorSeriesBrokerageFrequencyComparison, None, None]:
        self._log(
            msg=f"Starting comparison with grouper {self.grouper.name}",
            verbose=verbose)
        for stage_max in range(n_stages):
            self._log(
                msg=f"\tMax stage m={stage_max}",
                verbose=verbose)
            for stage in range(n_stages - 2):
                stage_next = stage + 1
                self._log(msg=f"Stage s={stage}, next stage {stage_next}", verbose=verbose)
                result = self.compute_comparison(stage_curr=stage, stage_next=stage_next, stage_max=stage_max, verbose=verbose, **kwargs)

                if result is None:
                    continue

                self._log(msg="Result:", verbose=verbose)
                self._log(msg=f"\tstage_curr=`{result.stage_curr}`", verbose=verbose)
                self._log(msg=f"\tstage_next=`{result.stage_next}`", verbose=verbose)
                self._log(msg=f"\tstage_max=`{result.stage_max}`", verbose=verbose)
                self._log(msg=f"\tgrouping_key=`{result.grouping_key}`", verbose=verbose)
                self._log(msg=f"\ttest_statistic=`{result.test_statistic:.2f}`", verbose=verbose)
                self._log(msg=f"\tp_value=`{result.p_value:.2f}`", verbose=verbose)
                if result.ci_low is not None:
                    self._log(msg=f"\tci_low=`{result.ci_low:.2f}`", verbose=verbose)
                else:
                    self._log(msg=f"\tci_low=`None`", verbose=verbose)

                if result.ci_high is not None:
                    self._log(msg=f"\tci_high=`{result.ci_high:.2f}", verbose=verbose)
                else:
                    self._log(msg=f"\tci_high=`None`", verbose=verbose)

                self._log(msg=f"\tn_x=`{result.n_x}`", verbose=verbose)
                self._log(msg=f"\tmu_x=`{result.mu_x:.2f}`", verbose=verbose)
                self._log(msg=f"\tstd_x=`{result.std_x:.2f}`", verbose=verbose)
                self._log(msg=f"\tn_y=`{result.n_y}`", verbose=verbose)
                self._log(msg=f"\tmu_y=`{result.mu_y:.2f}`", verbose=verbose)
                self._log(msg=f"\tstd_y=`{result.std_y:.2f}`", verbose=verbose)
                yield result

class CollaboratorSeriesRateStageCorrelation(CollaboratorSeriesRateStageComparison):
    def compute_comparison(
            self,
            stage_curr: int, stage_next: int, stage_max: int,
            verbose: bool = True, grouping_key: Union[str, None] = None, **kwargs)\
                -> MetricCollaboratorSeriesBrokerageRateComparison:

        a_idc_curr, a_vals_stage_curr =\
            self.get_values(stage_curr=stage_curr, stage_max=stage_max, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(s)={len(a_vals_stage_curr)}")
        a_idc_next, a_vals_stage_next =\
            self.get_values(stage_curr=stage_next, stage_max=stage_max, verbose=verbose, grouping_key=grouping_key)
        self._log(f"len(s+1)={len(a_vals_stage_next)}")

        if len(a_vals_stage_curr) == 0 or len(a_vals_stage_next) == 0:
            return None

        # Get indices of collaborators which participate in both stages
        _, a_mask_idc_curr_is, a_mask_idc_next_is =\
            np.intersect1d(a_idc_curr, a_idc_next,
                assume_unique=True,
                return_indices=True)

        # Filter out collaborators which do not participate in both stages
        a_idc_curr, a_vals_stage_curr =\
            a_idc_curr[a_mask_idc_curr_is], a_vals_stage_curr[a_mask_idc_curr_is]
        a_idc_next, a_vals_stage_next =\
            a_idc_next[a_mask_idc_next_is], a_vals_stage_next[a_mask_idc_next_is]

        # Align value arrays to compute correlation
        a_vals_stage_curr = a_vals_stage_curr[np.argsort(a_idc_curr)]
        a_vals_stage_next = a_vals_stage_next[np.argsort(a_idc_next)]
        self._log(f"len(s_is)={len(a_vals_stage_curr)}")
        self._log(f"len(s_is + 1)={len(a_vals_stage_next)}")

        _, t, p, ci = self._perform_test(a_vals_stage_next, a_vals_stage_curr)

        return MetricCollaboratorSeriesBrokerageRateComparison(
            id_metric_configuration=self.id_metric_config_comparison,
            stage_next=stage_next,
            stage_curr=stage_curr,
            stage_max=stage_max,
            grouping_key=grouping_key,
            test_statistic=t,
            p_value=p,
            ci_low=ci.low if ci is not None else None,
            ci_high=ci.high if ci is not None else None,
            n_x=len(a_vals_stage_next),
            mu_x=np.mean(a_vals_stage_next),
            std_x=np.std(a_vals_stage_next),
            n_y=len(a_vals_stage_curr),
            mu_y=np.mean(a_vals_stage_curr),
            std_y=np.std(a_vals_stage_curr),
        )
