import pandas as pd
import datetime as pydatetime
import os
import logging

logger = logging.getLogger(__name__)



class Findings():
    """
    This class manages the resulting output of the strategy ...
    """

    _columns = ['date',
                'symbol',
                'entry attempt',
                'entry style',
                'max risk',
                'risk price',
                'entry price',
                'entry time',
                'weight',
                '# shares',
                '$ shares risk',
                'cover price',
                'cover time',
                'entry-risk spread',
                'success / fail',
                'resulting r:r'
                ]

    backtest_results_dict = {}  # TODO: change the name of this to backtest results
    backtest_results_df = pd.DataFrame(columns=_columns)  # TODO: change the name of this to backtest results

    entry_queue: dict = {
        '1 min today': pd.DataFrame(),
        '2 min 2 days': pd.DataFrame(),
        '3 min 2 days': pd.DataFrame(),
        '5 min 3 days': pd.DataFrame(),
        '10 min 1 week': pd.DataFrame(),
        '15 min 1 week': pd.DataFrame(),
        '30 min 2 weeks': pd.DataFrame(),
        '1 hour 1 month': pd.DataFrame(),
        '4 hour 3 months': pd.DataFrame(),
    }

    def __init__(self) -> None:
        pass

    # def organize_backtest_results(self):
    #     if self.backtest_results_dict:  # if pattern list is not empty, place the pattern results into folder
    #         for res in self.backtest_results_dict:
    #             if not self.backtest_results_df.empty:
    #                 self.backtest_results_df = pd.concat([self.backtest_results_df, pd.DataFrame.from_dict(self.backtest_results_dict[res]).T])
    #             else:
    #                 self.backtest_results_df = pd.DataFrame.from_dict(self.backtest_results_dict[res]).T
    #     return self.backtest_results_df
    #
    # def backtest_results_to_csv(self):
    #     if not self.backtest_results_df.empty:
    #         self.backtest_results_df.to_csv(settings.BACKTEST_RESULTS_FOLDER_PATH +
    #                                         datetime.now().strftime('%Y-%m-%d') + "_backtest_" + 'verma_pattern' + ".csv")
    #     else:
    #         self.organize_backtest_results()
    #         self.backtest_results_df.to_csv(settings.BACKTEST_RESULTS_FOLDER_PATH +
    #                                         datetime.now().strftime('%Y-%m-%d') + "_backtest_" + 'verma_pattern' + ".csv")

    @classmethod
    def empty_findings_df(cls):
        return cls.backtest_results_df.iloc[[]]

    @classmethod
    def append_findings(cls, results: dict):
        cls.backtest_results_df = cls.backtest_results_df.append(results, ignore_index=True)
        return

    @classmethod
    def export(cls, plot_path):
        if not cls.backtest_results_df.empty:
            if not os.path.isdir(f"{plot_path}"):  # is path directory even created?
                os.makedirs(f"{plot_path}", mode=777)
            path_array = plot_path.split("/")
            cls.backtest_results_df.to_csv(f"{plot_path}findings_{path_array[-4]}_{path_array[-3]}_{path_array[-2]}.csv")
        else:
            logger.info(
                f"No Results"
            )
        return