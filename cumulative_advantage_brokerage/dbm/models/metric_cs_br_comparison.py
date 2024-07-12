from sqlalchemy import Column, Integer, Float, Index, String

from .metric_mixin import MetricMixin

class MetricCollaboratorSeriesBrokerageRateComparison(MetricMixin):
    __tablename__ = "metric_collaborator_series_brokerage_rate_comparison"

    stage_curr = Column(Integer)
    stage_next = Column(Integer)
    stage_max = Column(Integer)
    grouping_key = Column(String(50), nullable=True)

    test_statistic = Column(Float)
    p_value = Column(Float)
    ci_low = Column(Float)
    ci_high = Column(Float)

    n_x = Column(Integer, nullable=True)
    mu_x = Column(Float, nullable=True)
    std_x = Column(Float, nullable=True)
    n_y = Column(Integer, nullable=True)
    mu_y = Column(Float, nullable=True)
    std_y = Column(Float, nullable=True)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"stage=`{self.stage}`,"
            f"max_stage_curr=`{self.max_stage_curr}`,"
            f"max_stage_next=`{self.max_stage_next}`,"
            f"grouping_key=`{self.grouping_key}`,"
            f"test_statistic=`{self.test_statistic:.2f}`,"
            f"p_value=`{self.p_value:.2f}`,"
            f"ci_low=`{self.ci_low:.2f}`,"
            f"ci_high=`{self.ci_high:.2f}`"
            f"n_x=`{self.n_x}`,"
            f"mu_x=`{self.mu_x:.2f}`,"
            f"std_x=`{self.std_x:.2f}`,"
            f"n_y=`{self.n_y}`,"
            f"mu_y=`{self.mu_y:.2f}`,"
            f"std_y=`{self.std_y:.2f}`)"
        )

    __table_args__ = (
        Index(
            "idx_metric_collaborator_series_brokerage_rate_comparison",
            "id_metric_configuration",
            "stage_curr",
            "stage_next",
            "stage_max",
            "grouping_key",
            unique=True
        ),
    )
