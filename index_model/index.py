import datetime as dt
import pandas as pd
import os

class IndexModel:
    def __init__(self) -> None:
        # Use relative path for portability
        file_path = os.path.join(os.path.dirname(__file__), "..", "data_sources", "stock_prices.csv")
        self.df = pd.read_csv(file_path)

        self.df['Date'] = pd.to_datetime(self.df['Date'], format='%d/%m/%Y')
        self.df.set_index('Date', inplace=True)
        self.df.sort_index(inplace=True)
        self.index_df = None

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        df = self.df
        df_range = df.loc[start_date:end_date]

        initial_index = 100
        index_values = {}
        month_base_prices = {}
        month_index_base = {}
        composition_history = {}

        def last_weekday(year, month):
            last_day = pd.Timestamp(year + int(month == 12), month % 12 + 1, 1) - pd.Timedelta(days=1)
            while last_day.weekday() >= 5:
                last_day -= pd.Timedelta(days=1)
            return last_day

        def get_first_and_second_weekdays(dates):
            weekdays = [d for d in dates if d.weekday() < 5]
            return weekdays[0], weekdays[1] if len(weekdays) > 1 else (weekdays[0], weekdays[0])

        # Composition on 31-Dec-2019
        composition_3112 = df.loc[pd.Timestamp('2019-12-31')].sort_values(ascending=False).head(3).index.tolist()

        for current_date in df_range.index:
            year, month = current_date.year, current_date.month
            month_dates = df_range[(df_range.index.year == year) & (df_range.index.month == month)].index
            first_wd, second_wd = get_first_and_second_weekdays(month_dates)

            if current_date == pd.Timestamp('2020-01-01'):
                index_values[current_date] = initial_index

            if current_date <= pd.Timestamp('2020-01-31'):
                base_prices = df.loc[pd.Timestamp('2020-01-01'), composition_3112]
                current_prices = df.loc[current_date, composition_3112]
                returns = current_prices / base_prices
                weighted_return = 0.5 * returns.iloc[0] + 0.25 * returns.iloc[1] + 0.25 * returns.iloc[2]
                index_values[current_date] = initial_index * weighted_return
                continue

            if current_date == first_wd and current_date != pd.Timestamp('2020-02-03'):
                prev_month = month - 1 if month > 1 else 12
                prev_year = year if month > 1 else year - 1
                two_months_ago = month - 2 if month > 2 else 12 + (month - 2)
                two_months_year = year if month > 2 else year - 1

                prev_month_dates = df_range[(df_range.index.year == prev_year) & (df_range.index.month == prev_month)].index
                prev_first_wd, _ = get_first_and_second_weekdays(prev_month_dates)

                last_wd_two_months_ago = last_weekday(two_months_year, two_months_ago)
                composition = df.loc[last_wd_two_months_ago].sort_values(ascending=False).head(3).index.tolist()

                base_prices = df.loc[prev_first_wd, composition]
                current_prices = df.loc[current_date, composition]
                index_base = index_values[prev_first_wd]

                returns = current_prices / base_prices
                weighted_return = 0.5 * returns.iloc[0] + 0.25 * returns.iloc[1] + 0.25 * returns.iloc[2]

                index_values[current_date] = index_base * weighted_return
                month_base_prices[month] = base_prices
                month_index_base[month] = index_values[current_date]
                composition_history[month] = composition
                continue

            if current_date == pd.Timestamp('2020-02-03'):
                base_prices = df.loc[pd.Timestamp('2020-01-01'), composition_3112]
                current_prices = df.loc[current_date, composition_3112]
                index_base = index_values[pd.Timestamp('2020-01-01')]
                returns = current_prices / base_prices
                weighted_return = 0.5 * returns.iloc[0] + 0.25 * returns.iloc[1] + 0.25 * returns.iloc[2]
                index_values[current_date] = index_base * weighted_return
                month_base_prices[month] = base_prices
                month_index_base[month] = index_values[current_date]
                continue

            if current_date == second_wd and current_date > pd.Timestamp('2020-02-03'):
                prev_month = month - 1 if month > 1 else 12
                prev_year = year if month > 1 else year - 1
                last_wd_prev_month = last_weekday(prev_year, prev_month)
                composition = df.loc[last_wd_prev_month].sort_values(ascending=False).head(3).index.tolist()
                base_prices = df.loc[first_wd, composition]
                index_base = index_values[first_wd]
                current_prices = df.loc[current_date, composition]
                returns = current_prices / base_prices
                weighted_return = 0.5 * returns.iloc[0] + 0.25 * returns.iloc[1] + 0.25 * returns.iloc[2]
                index_values[current_date] = index_base * weighted_return
                month_base_prices[month] = base_prices
                month_index_base[month] = index_base
                composition_history[month] = composition
                continue

            if month in month_base_prices and current_date > second_wd:
                base_prices = month_base_prices[month]
                index_base = month_index_base[month]
                current_prices = df.loc[current_date, base_prices.index.tolist()]
                returns = current_prices / base_prices
                weighted_return = 0.5 * returns.iloc[0] + 0.25 * returns.iloc[1] + 0.25 * returns.iloc[2]
                index_values[current_date] = index_base * weighted_return
                continue

            # Carry forward last known index value
            prev_date = current_date - pd.Timedelta(days=1)
            while prev_date not in index_values:
                prev_date -= pd.Timedelta(days=1)
            index_values[current_date] = index_values[prev_date]

        # Save final DataFrame
        self.index_df = pd.DataFrame(index_values.items(), columns=['Date', 'Index_Value_Unrounded'])
        self.index_df['Index_Value_Rounded'] = self.index_df['Index_Value_Unrounded'].round(2)

    def export_values(self, file_name: str) -> None:
        if self.index_df is not None:
            self.index_df.to_csv(file_name, index=False)
        else:
            raise ValueError("Index has not been calculated. Please run calc_index_level() first.")
